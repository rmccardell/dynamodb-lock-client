using System;
using System.Collections.Generic;
using System.Text;

namespace IMODDesigns.Exceptions
{
    public class LockTableDoesNotExistException : Exception
    {
        public LockTableDoesNotExistException(string message, Exception innerException) : base(message, innerException)
        {

        }

        public LockTableDoesNotExistException(string message) : base(message)
        {

        }
    }
}
