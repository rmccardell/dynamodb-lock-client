using System;
using System.Collections.Generic;
using System.Text;
using Amazon.DynamoDBv2;
using IMODDesigns.Utilities;

namespace IMODDesigns.Models
{
    public class LockItem
    {
     
        public string RecordVersionNumber { get; private set; }

        public string Owner { get; private set; }

        public bool IsReleased { get; }

        public DateTime LookUpTime { get; private set; }

        
        public long LeaseDuration { get; private set; }


        public long TimeSinceLastLookUp
        {
            get
            {
                double timeElapsedSinceLookUp = (DateTime.Now - LookUpTime).TotalMilliseconds;

                return (long) timeElapsedSinceLookUp;
            }
        }

        public bool IsExpired
        {
            get
            {
              
                double timeElapsedSinceLookUp = (DateTime.Now - LookUpTime).TotalMilliseconds;

                return timeElapsedSinceLookUp > LeaseDuration;
            }
        }

        public string UniqueIdentifier { get; }

        public LockItem(string partitionKey, string owner, string recordVersionNumber, DateTime lookUpTime, long? leaseDuration, bool isReleased = false)
        {
            UniqueIdentifier = partitionKey;
            LookUpTime = lookUpTime;
            RecordVersionNumber = recordVersionNumber;
            LeaseDuration = leaseDuration ?? Defaults.DefaultLeaseDuration;

            Owner = owner;
  
            IsReleased = isReleased;
        }


      public void UpdatedRecordVersionNumber(string recordVersionNumber, DateTime lastUpdateOfLock, long leaseDurationToEnsureInMilliseconds)
      {
          RecordVersionNumber = recordVersionNumber;
          LookUpTime = lastUpdateOfLock;
          LeaseDuration = leaseDurationToEnsureInMilliseconds;
      }
    }
}
