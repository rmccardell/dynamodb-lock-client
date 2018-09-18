using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Runtime.ExceptionServices;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using IMODDesigns.Exceptions;
using IMODDesigns.Models;
using IMODDesigns.Utilities;

// ReSharper disable once CheckNamespace
namespace IMODDesigns
{
    public class AmazonDynamoDbLockClient
    {

        protected const string HeartBeatPeriodError =
            "Heartbeat period must be no more than half the length of the Lease Duration, "
            + "or locks might expire due to the heartbeat thread taking too long to update them (recommendation is to make it much greater, for example "
            + "4+ times greater)";

        protected const String DefaultPartitionKeyName = "key";
        private const int DefaultBufferMs = 1000;
        protected const string Data = "data";
        protected const string OwnerName = "ownerName";
        protected const string LeaseDuration = "leaseDuration";
        protected const string RecordVersionNumber = "recordVersionNumber";
        protected const string DeleteOnRelease = "deleteOnRelease";
        protected const string IsReleased = "isReleased";

        protected static String SkPathExpressionVariable = "#sk";
        protected static String PkPathExpressionVariable = "#pk";
        protected static String NewRvnValueExpressionVariable = ":newRvn";
        protected static String LeaseDurationPathValueExpressionVariable = "#ld";
        protected static String LeaseDurationValueExpressionVariable = ":ld";
        protected static String RvnPathExpressionVariable = "#rvn";
        protected static String RvnValueExpressionVariable = ":rvn";
        protected static String OwnerNamePathExpressionVariable = "#on";
        protected static String OwnerNameValueExpressionVariable = ":on";
        protected static String DataPathExpressionVariable = "#d";
        protected static String DataValueExpressionVariable = ":d";
        protected static String IsReleasedPathExpressionVariable = "#ir";
        protected static String IsReleasedValueExpressionVariable = ":ir";

        protected static string AcquireLockThatDoesntExistOrIsReleasedCondition =
            $"attribute_not_exists({PkPathExpressionVariable}) OR (attribute_exists({PkPathExpressionVariable}) AND {IsReleasedPathExpressionVariable} = {IsReleasedValueExpressionVariable})";

        protected static string PkExistsAndSkExistsAndRvnIsTheSameCondition =
            $"attribute_exists({PkPathExpressionVariable}) AND attribute_exists({SkPathExpressionVariable}) AND {RvnPathExpressionVariable} = {RvnValueExpressionVariable}";

        protected static string PkExistsAndSkExistsAndOwnerNameSameAndRvnSameCondition =
            $"{PkExistsAndSkExistsAndRvnIsTheSameCondition} AND {OwnerNamePathExpressionVariable} = {OwnerNameValueExpressionVariable}";


        protected static string PkExistsAndRvnIsTheSameCondition =
            $"attribute_exists({PkPathExpressionVariable}) AND {RvnPathExpressionVariable} = {RvnValueExpressionVariable}";


        protected static string PkExistsAndOwnerNameSameAndRvnSameCondition =
            $"{PkExistsAndRvnIsTheSameCondition} AND {OwnerNamePathExpressionVariable} = {OwnerNameValueExpressionVariable}";

        protected static string UpdateIsReleased =
            $"SET {IsReleasedPathExpressionVariable} = {IsReleasedValueExpressionVariable}";


        protected static string UpdateIsReleasedAndData =
            $"{UpdateIsReleased}, {DataPathExpressionVariable} = {DataValueExpressionVariable}";

        protected static string UpdateLeaseDurationAndRvn =
            $"SET {LeaseDurationPathValueExpressionVariable} = {LeaseDurationValueExpressionVariable}, {RvnPathExpressionVariable} = {NewRvnValueExpressionVariable}";

        protected static string UpdateLeaseDurationAndRvnAndRemoveData =
            $"{UpdateLeaseDurationAndRvn} REMOVE {DataPathExpressionVariable}";

        protected static string UpdateLeaseDurationAndRvnAndData =
            $"{UpdateLeaseDurationAndRvn}, {DataPathExpressionVariable} = {DataValueExpressionVariable}";


        protected static string IsReleasedValue = "1";
        protected static AttributeValue IsReleasedAttributeValue = new AttributeValue(IsReleasedValue);

        private readonly ConcurrentDictionary<string, LockItem> _locks = new ConcurrentDictionary<string, LockItem>();

        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

        private readonly HashSet<TableStatus> _availableTableStatuses = new HashSet<TableStatus>
        {
            TableStatus.ACTIVE,
            TableStatus.UPDATING
        };

        private readonly Object _threadLock = new Object();


        private readonly AmazonDynamoDBClient _client;
        private readonly string _partitionKeyName;
        private readonly string _tableName;
        private readonly long _leaseDuration;
        private readonly long _heartbeatPeriod;
        private readonly string _ownerName;


        public AmazonDynamoDbLockClient(AmazonDynamoDBClient client, string tableName, long? leaseDuration,
            long? heartbeatPeriod)
        {
            _client = client;
            _partitionKeyName = DefaultPartitionKeyName;
            _tableName = tableName;
            _leaseDuration = leaseDuration ?? Defaults.DefaultLeaseDuration;
            _heartbeatPeriod = heartbeatPeriod ?? Defaults.DefaultHeartbeatPeriod;
            _ownerName = GenerateOwnerNameFromLocalhost();

            if (_leaseDuration < 2 * _heartbeatPeriod)
            {
                throw new InvalidOperationException(HeartBeatPeriodError);
            }

            VerifyLockTable();

            Task.Run(() => StartHeartBeat(_cancellationTokenSource.Token));
        }


        private void VerifyLockTable()
        {
            if (!LockTableExists())
            {
                CreateLockTableInDynamoDB(_client, _tableName, new ProvisionedThroughput(5, 5), _partitionKeyName);
            }
        }

        private bool LockTableExists()
        {
            try
            {
                DescribeTableResponse result = _client.DescribeTableAsync(new DescribeTableRequest(_tableName))
                    .Result;


                if (result.HttpStatusCode != HttpStatusCode.OK)
                    return false;

                var tableActiveOrUpdated = _availableTableStatuses.Contains(result.Table.TableStatus);

                return tableActiveOrUpdated;
            }
            catch (ResourceNotFoundException)
            {
                return false;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static void CreateLockTableInDynamoDB(AmazonDynamoDBClient client, string lockTableName,
            ProvisionedThroughput provisionedThroughput, string partitionKeyName)
        {
            KeySchemaElement partitionKeyElement = new KeySchemaElement(partitionKeyName, KeyType.HASH);
            List<KeySchemaElement> keySchema = new List<KeySchemaElement> { partitionKeyElement };

            List<AttributeDefinition> attributeDefinitions =
                new List<AttributeDefinition> { new AttributeDefinition(partitionKeyName, ScalarAttributeType.S) };

            //if (!string.IsNullOrEmpty(sortKeyName))
            //{
            //    KeySchemaElement sortKeyElement = new KeySchemaElement(sortKeyName, KeyType.RANGE);
            //    keySchema.Add(sortKeyElement);

            //    attributeDefinitions.Add(new AttributeDefinition(sortKeyName, ScalarAttributeType.S));
            //}

            CreateTableRequest createTableRequest =
                new CreateTableRequest(lockTableName, keySchema, attributeDefinitions, provisionedThroughput);

            var createTableResponse = client.CreateTableAsync(createTableRequest).Result;

            if (createTableResponse.HttpStatusCode != HttpStatusCode.OK)
            {

                //todo: fill this out with identifying information for the error
                throw new LockTableCreationFailedException("failed");
            }
        }


        private void AssertLockTableExists()
        {
            bool exists;

            try
            {
                exists = this.LockTableExists();
            }
            catch (Exception e)
            {
                throw new LockTableDoesNotExistException("Lock table " + _tableName + " does not exist", e);
            }

            if (!exists)
            {
                throw new LockTableDoesNotExistException("Lock table " + _tableName + " does not exist");
            }
        }

        private static string GenerateOwnerNameFromLocalhost()
        {
            try
            {
                return Dns.GetHostName() + Guid.NewGuid();
            }
            catch (Exception)
            {
                return Guid.NewGuid().ToString();
            }
        }


        private string GenerateRecordVersionNumber()
        {
            return Guid.NewGuid().ToString();
        }


        private GetItemResponse GetItem(string key)
        {
            var dynamoDbKey =
                new Dictionary<string, AttributeValue> { { _partitionKeyName, new AttributeValue { S = key } } };

            GetItemRequest getItemRequest = new GetItemRequest(_tableName, dynamoDbKey);

            return _client.GetItemAsync(getItemRequest, CancellationToken.None).Result;
        }



        private LockItem CreateLockFromDbAttribute(Dictionary<string, AttributeValue> item, DateTime lookUpTime)
        {

            if (!item.ContainsKey(OwnerName) || !item.ContainsKey(LeaseDuration) ||
                !item.ContainsKey(_partitionKeyName) ||
                !item.ContainsKey(RecordVersionNumber))
            {
                throw new InvalidOperationException("cannot create LockItem from dbItem");
            }

            string ownerName = null;
            long leaseDuration = 0;
            string recordVersionNumber = null;
            string partitionKey = null;
            bool isReleased;
            //bool deleteOnRelease = true;


            if (item.TryGetValue(OwnerName, out var ownerNameAttributeValue))
            {
                ownerName = ownerNameAttributeValue.S;
            }

            if (item.TryGetValue(_partitionKeyName, out var partitionKeyAttributeValue))
            {
                partitionKey = partitionKeyAttributeValue.S;
            }

            if (item.TryGetValue(LeaseDuration, out var leaseDurationAttributeValue))
            {
                var leaseDurationValue = leaseDurationAttributeValue.S;
                leaseDuration = long.Parse(leaseDurationValue);
            }

            if (item.TryGetValue(RecordVersionNumber, out var recordVersionAttributeValue))
            {
                recordVersionNumber = recordVersionAttributeValue.S;
            }


            if (item.TryGetValue(DeleteOnRelease, out var deleteOnReleaseAttributeValue))
            {
                // deleteOnRelease = deleteOnReleaseAttributeValue.BOOL;
            }


            isReleased = item.ContainsKey(IsReleased);

            var lockItem = new LockItem(partitionKey, ownerName, recordVersionNumber, lookUpTime, leaseDuration,
                isReleased);

            return lockItem;
        }

        private Dictionary<string, AttributeValue> BuildLockItem(string partitionkey, string recordVersionNumber)
        {

            Dictionary<string, AttributeValue> item = new Dictionary<string, AttributeValue>
            {
                {_partitionKeyName, new AttributeValue {S = partitionkey}},
                {OwnerName, new AttributeValue {S = _ownerName}},
                {LeaseDuration, new AttributeValue {S = _leaseDuration.ToString()}},
                {RecordVersionNumber, new AttributeValue {S = recordVersionNumber}}
            };

            return item;
        }

        private LockItem GetExistingLock(string partitionKey)
        {

            try
            {

                if (string.IsNullOrEmpty(partitionKey))
                {
                    throw new ArgumentException("partitionKey must not be empty");
                }

                var item = GetItem(partitionKey);

                if (item.HttpStatusCode != HttpStatusCode.OK)
                {
                    return null;
                }


                DateTime lookUpTime = DateTime.Now;

                Console.WriteLine($"existing item lookuptime set to { TimeSpan.FromTicks(lookUpTime.Ticks)} ");

                var lockItem = CreateLockFromDbAttribute(item.Item, lookUpTime);
                return lockItem;
            }
            catch (Exception)
            {
                //todo: log this
                return null;
            }


        }

        private LockItem AddLockItemToDynamo(string partitionKey, string recordVersionNumber,
            PutItemRequest putItemRequest)
        {
            long lastUpdatedTime = LockClientUtils.TimeStamp();


            PutItemResponse response = _client.PutItemAsync(putItemRequest).Result;

            if (response.HttpStatusCode == HttpStatusCode.OK)
            {
                //todo: do something with this
            }

            LockItem lockItem =
                new LockItem(partitionKey, _ownerName, recordVersionNumber, DateTime.Now, null);

            _locks.TryAdd(lockItem.UniqueIdentifier, lockItem);

            return lockItem;
        }

        public LockItem AddAndWatchNewOrReleasedLock(string partitionKey, string recordVersionNumber,
            Dictionary<string, AttributeValue> item)
        {
            Dictionary<string, string> expressionAttributeNames = new Dictionary<string, string>
            {
                {PkPathExpressionVariable, _partitionKeyName},
                {IsReleasedPathExpressionVariable, IsReleased}
            };

            Dictionary<string, AttributeValue> expressionAttributeValues =
                new Dictionary<string, AttributeValue> { { IsReleasedValueExpressionVariable, IsReleasedAttributeValue } };


            PutItemRequest putItemRequest = new PutItemRequest(_tableName, item)
            {
                ConditionExpression = AcquireLockThatDoesntExistOrIsReleasedCondition,
                ExpressionAttributeNames = expressionAttributeNames,
                ExpressionAttributeValues = expressionAttributeValues
            };


            long lastUpdatedTime = LockClientUtils.TimeStamp();


            PutItemResponse response = _client.PutItemAsync(putItemRequest).Result;

            if (response.HttpStatusCode != HttpStatusCode.OK)
            {
                //todo: do something with this
            }

            LockItem lockItem =
                new LockItem(partitionKey, _ownerName, recordVersionNumber, DateTime.Now, null);

            _locks.TryAdd(lockItem.UniqueIdentifier, lockItem);

            return lockItem;

        }


        private LockItem UpsertExpiredLock(string key, string recordVersionNumber)
        {

            var item = BuildLockItem(key, recordVersionNumber);

            string conditionalExpression = null;
            Dictionary<string, AttributeValue> expressionAttributeValues =
                new Dictionary<string, AttributeValue>
                {
                    {RvnValueExpressionVariable, new AttributeValue {S = recordVersionNumber}}
                };

            Dictionary<string, string> expressionAttributeNames = new Dictionary<string, string>
            {
                {PkPathExpressionVariable, _partitionKeyName},
                {RvnPathExpressionVariable, RecordVersionNumber}
            };

            conditionalExpression = PkExistsAndRvnIsTheSameCondition;

            PutItemRequest putItemRequest =
                new PutItemRequest(_tableName, item)
                {
                    ExpressionAttributeNames = expressionAttributeNames,
                    ExpressionAttributeValues = expressionAttributeValues,
                    ConditionExpression = conditionalExpression
                };

            DateTime lookUpTime = DateTime.Now;

            PutItemResponse response = _client.PutItemAsync(putItemRequest).Result;

            if (response.HttpStatusCode == HttpStatusCode.OK)
            {
                var lockItem = CreateLockFromDbAttribute(item, lookUpTime);

                _locks.TryAdd(lockItem.UniqueIdentifier, lockItem);

                return lockItem;
            }


            return null;
        }

        private void ReleaseAllLocks()
        {
            Dictionary<string, LockItem> locks = new Dictionary<string, LockItem>(_locks);

            lock (_threadLock)
            {
                foreach (var lockEntry in locks)
                {
                    ReleaseLock(lockEntry.Value);
                }
            }
        }

        public void Close()
        {
            ReleaseAllLocks();
        }

        private async Task<LockItem> AcquireLockItemAsync(string partitionKey, long? millisecondsToWait,
            long? refreshPeriodInMilliseconds)
        {
            long timeToWait = millisecondsToWait ?? DefaultBufferMs;
            long refreshPeriod = refreshPeriodInMilliseconds ?? DefaultBufferMs;

            Stopwatch stopWatch = new Stopwatch();

            stopWatch.Start();

            LockItem lockTryingToBeAcquired = null;
            bool alreadySleptOnceForOneLeasePeriod = false;


            while (true)
            {
                try
                {
                    try
                    {
                        var existingLockItem = GetExistingLock(partitionKey);

                        if (existingLockItem == null || existingLockItem.IsReleased)
                        {
                            var recordVersionNumber = GenerateRecordVersionNumber();
                            var item = BuildLockItem(partitionKey, recordVersionNumber);

                            var lockItem = AddAndWatchNewOrReleasedLock(partitionKey, recordVersionNumber, item);

                            var lockAcquiredTime = stopWatch.ElapsedMilliseconds;

                            Console.WriteLine($"lock acquired in {lockAcquiredTime / 1000}");

                            return lockItem;
                        }

                        if (lockTryingToBeAcquired == null)
                        {
                            //this branch of logic only happens once, in the first iteration of the while loop
                            //lockTryingToBeAcquired only ever gets set to non-null values after this point.
                            //so it is impossible to get in this
                            /*
                             * Someone else has the lock, and they have the lock for LEASE_DURATION time. At this point, we need
                             * to wait at least LEASE_DURATION milliseconds before we can try to acquire the lock.
                             */
                            lockTryingToBeAcquired = existingLockItem;
                            if (!alreadySleptOnceForOneLeasePeriod)
                            {
                                alreadySleptOnceForOneLeasePeriod = true;
                                timeToWait += existingLockItem.LeaseDuration;

                                Console.WriteLine($"will wait {timeToWait / 1000} seconds for lock");
                            }
                        }
                        else
                        {
                            if (lockTryingToBeAcquired.RecordVersionNumber.Equals(existingLockItem.RecordVersionNumber))
                            {
                                /* If the version numbers match, then we can acquire the lock, assuming it has already expired */
                                if (lockTryingToBeAcquired.IsExpired)
                                {
                                    var lockItem = UpsertExpiredLock(partitionKey,
                                        lockTryingToBeAcquired.RecordVersionNumber);

                                    var lockAcquiredTime = stopWatch.ElapsedMilliseconds;

                                    Console.WriteLine($"lock acquired in {lockAcquiredTime / 1000}");

                                    return lockItem;
                                }
                            }
                            else
                            {
                                /*
                                 * If the version number changed since we last queried the lock, then we need to update
                                 * lockTryingToBeAcquired as the lock has been refreshed since we last checked
                                 */
                                lockTryingToBeAcquired = existingLockItem;
                            }
                        }
                    }
                    catch (ConditionalCheckFailedException conditionalCheckFailedException)
                    {
                        /* Someone else acquired the lock while we tried to do so, so we throw an exception */
                        // logger.debug("Someone else acquired the lock", conditionalCheckFailedException);
                        throw new LockNotGrantedException("Could not acquire lock because someone else acquired it: ",
                            conditionalCheckFailedException);


                    }
                    catch (AmazonClientException amazonClientException)
                    {
                        /* This indicates that we were unable to successfully connect and make a service call to DDB. Often
                         * indicative of a network failure, such as a socket timeout. We retry if still within the time we
                         * can wait to acquire the lock.
                         */
                        //  logger.warn("Could not acquire lock because of a client side failure in talking to DDB", amazonClientException);

                    }
                }
                catch (LockNotGrantedException lockNotGrantedException)
                {

                    if (stopWatch.ElapsedMilliseconds > timeToWait)
                    {
                        //logger.debug("This client waited more than millisecondsToWait=" + millisecondsToWait
                        //                                                                + " ms since the beginning of this acquire call.", x);
                        //throw x;

                        throw;
                    }
                }


                double timeElapsed = Math.Round((double)stopWatch.ElapsedMilliseconds, MidpointRounding.ToEven);

                Console.WriteLine($"Time elapsed {timeElapsed * 0.001} seconds");

                if (timeElapsed > timeToWait)
                {
                    Console.WriteLine($"item timesince last lookup {lockTryingToBeAcquired.TimeSinceLastLookUp}");
                    Console.WriteLine($"lock not granted after {timeElapsed / 1000} seconds of waiting. ");

                    throw new LockNotGrantedException(
                        "Didn't acquire lock after sleeping for " +
                        (timeElapsed) + " milliseconds", null);
                }

                //logger.trace("Sleeping for a refresh period of " + refreshPeriodInMilliseconds + " ms");

                Console.WriteLine("Sleeping for a refresh period of " + refreshPeriod + " ms");

                //Thread.Sleep((int)refreshPeriod);

                var sleepStartTime = stopWatch.ElapsedMilliseconds;

                await Task.Delay((int)refreshPeriod);

                var sleepEndTime = stopWatch.ElapsedMilliseconds;

                var sleepTime = sleepEndTime - sleepStartTime;

                Console.WriteLine($"slept {sleepTime / 1000} seconds");
            }
        }


        public LockItem TryAcquireLockItem(string partitionKey, long? millisecondsToWait,
            long? refreshPeriodInMilliseconds, bool retryOnFirstFail = true)
        {
            LockItem lockItem = null;

            var acquireAction = new Func<LockItem>(() =>
            {

                try
                {
                    LockItem item = null;

                    Task.Run(async () =>
                    {
                        item = await AcquireLockItemAsync(partitionKey, millisecondsToWait, refreshPeriodInMilliseconds);
                    }).Wait();

                    return item;
                }
                catch (AggregateException ae)
                {
                    foreach (var e in ae.Flatten().InnerExceptions)
                    {
                        ExceptionDispatchInfo.Capture(e).Throw();
                    }
                }

                return null;
            });

            try
            {
                lockItem = acquireAction();
            }
            catch (Exception e)
            {

                if (retryOnFirstFail)
                {
                    retryOnFirstFail = false;
                    lockItem = acquireAction();
                }
                else
                {
                    ExceptionDispatchInfo.Capture(e).Throw();
                }
            }


            return lockItem;
        }


        public bool ReleaseLock(LockItem item)
        {
            if (!item.Owner.Equals(_ownerName))
            {
                return false;
            }

            lock (_threadLock)
            {
                try
                {
                    _locks.TryRemove(item.UniqueIdentifier, out var removedLockItem);

                    Dictionary<string, AttributeValue> expressionAttributeValues =
                        new Dictionary<string, AttributeValue>
                        {
                            {RvnValueExpressionVariable, new AttributeValue {S = item.RecordVersionNumber}},
                            {OwnerNameValueExpressionVariable, new AttributeValue {S = item.Owner}}
                        };

                    Dictionary<string, string> expressionAttributeNames = new Dictionary<string, string>
                    {
                        {PkPathExpressionVariable, _partitionKeyName},
                        {OwnerNamePathExpressionVariable, OwnerName},
                        {RvnPathExpressionVariable, RecordVersionNumber}
                    };

                    var conditionalExpression = PkExistsAndOwnerNameSameAndRvnSameCondition;

                    Dictionary<string, AttributeValue> key = new Dictionary<string, AttributeValue>
                    {
                        {
                            _partitionKeyName, new AttributeValue
                            {
                                S = item.UniqueIdentifier
                            }
                        }
                    };

                    string updateExpression = null;
                    expressionAttributeNames.Add(IsReleasedPathExpressionVariable, IsReleased);
                    expressionAttributeValues.Add(IsReleasedValueExpressionVariable, IsReleasedAttributeValue);

                    updateExpression = UpdateIsReleased;


                    UpdateItemRequest updateItemRequest =
                        new UpdateItemRequest(_tableName, key, null)
                        {
                            ConditionExpression = conditionalExpression,
                            UpdateExpression = updateExpression,
                            ExpressionAttributeNames = expressionAttributeNames,
                            ExpressionAttributeValues = expressionAttributeValues
                        };


                    UpdateItemResponse response =
                        _client.UpdateItemAsync(updateItemRequest, CancellationToken.None).Result;


                }
                catch (ConditionalCheckFailedException conditionalCheckFailedException)
                {
                    // logger.debug("Someone else acquired the lock before you asked to release it", conditionalCheckFailedException);
                    return false;
                }
                catch (AmazonClientException amazonClientException)
                {
                    throw;
                }
            }

            return true;
        }


        public void SendHeartbeat(LockItem item)
        {

            long leaseDurationToEnsureInMilliseconds = _leaseDuration;


            if (item.IsExpired || !item.Owner.Equals(this._ownerName) || item.IsReleased)
            {
                _locks.TryRemove(item.UniqueIdentifier, out var removedLockItem);

                throw new LockNotGrantedException("Cannot send heartbeat because lock is not granted", null);
            }

            lock (_threadLock)
            {
                string recordVersionNumber = GenerateRecordVersionNumber();
                string conditionalExpression = null;
                string updateExpression = null;

                Dictionary<string, AttributeValue> expressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    {RvnValueExpressionVariable, new AttributeValue {S = item.RecordVersionNumber}},
                    {OwnerNameValueExpressionVariable, new AttributeValue {S = item.Owner}}
                };

                Dictionary<string, string> expressionAttributeNames = new Dictionary<string, string>
                {
                    {PkPathExpressionVariable, _partitionKeyName},
                    {LeaseDurationPathValueExpressionVariable, LeaseDuration},
                    {RvnPathExpressionVariable, RecordVersionNumber},
                    {OwnerNamePathExpressionVariable, OwnerName}
                };


                expressionAttributeValues.Add(NewRvnValueExpressionVariable, new AttributeValue { S = recordVersionNumber });
                expressionAttributeValues.Add(LeaseDurationValueExpressionVariable, new AttributeValue
                {
                    S = _leaseDuration.ToString()
                });


                conditionalExpression = PkExistsAndOwnerNameSameAndRvnSameCondition;
                updateExpression = UpdateLeaseDurationAndRvn;

                var key = new Dictionary<string, AttributeValue>
                {
                    {_partitionKeyName, new AttributeValue {S = item.UniqueIdentifier}}
                };

                UpdateItemRequest updateItemRequest =
                    new UpdateItemRequest(_tableName, key, null)
                    {
                        ConditionExpression = conditionalExpression,
                        UpdateExpression = updateExpression,
                        ExpressionAttributeValues = expressionAttributeValues,
                        ExpressionAttributeNames = expressionAttributeNames
                    };



                try
                {

                    UpdateItemResponse response = _client.UpdateItemAsync(updateItemRequest).Result;

                    if (response.HttpStatusCode != HttpStatusCode.OK)
                        throw new LockNotGrantedException("failed to updated database", null);

                    item.UpdatedRecordVersionNumber(recordVersionNumber, DateTime.Now,
                        leaseDurationToEnsureInMilliseconds);

                    Console.WriteLine($"updated lock item : {item.UniqueIdentifier} with new RVN: {recordVersionNumber} and lease {_leaseDuration / 1000} seconds");
                }
                catch (ConditionalCheckFailedException conditionalCheckFailedException)
                {

                    //    logger.debug("Someone else acquired the lock, so we will stop heartbeating it", conditionalCheckFailedException);
                    _locks.TryRemove(item.UniqueIdentifier, out var lockToRemove);
                    throw new LockNotGrantedException("Someone else acquired the lock, so we will stop heartbeating it",
                        conditionalCheckFailedException);
                }

            }
        }

        private long Heartbeat()
        {
            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();

                Dictionary<string, LockItem> copyOfLocks = new Dictionary<string, LockItem>(_locks);

                foreach (var watchedLocks in copyOfLocks)
                {
                    try
                    {

                        SendHeartbeat(watchedLocks.Value);

                    }
                    catch (LockNotGrantedException lockNotGrantedException)
                    {
                        //logger.debug("Hearbeat failed for " + lockEntry, x);
                    }
                    catch (Exception ex)
                    {
                        //logger.warn("Exception sending heartbeat for " + lockEntry, x);
                    }

                }

                long timeElapsed = sw.ElapsedMilliseconds;

                return Math.Max(_heartbeatPeriod - timeElapsed, 0);

            }
            catch (Exception)
            {
                //logger.warn("Exception sending heartbeat", x);
            }

            return _heartbeatPeriod;
        }




        public async Task StartHeartBeat(CancellationToken cancellationToken)
        {
            while (true)
            {
                long sleepTime = _heartbeatPeriod;

                await Task.Run(() => { sleepTime = Heartbeat(); }, cancellationToken);
                await Task.Delay(TimeSpan.FromMilliseconds(sleepTime), cancellationToken);
            }
        }

    }
}
