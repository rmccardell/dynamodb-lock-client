using System;

namespace IMODDesigns.Exceptions
{
    public class LockNotGrantedException: Exception
    {
        public LockNotGrantedException(string message, Exception innerException) : base(message, innerException)
        {

        }
    }
}
