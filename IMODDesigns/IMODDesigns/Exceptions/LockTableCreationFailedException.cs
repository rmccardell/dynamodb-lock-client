using System;
using System.Collections.Generic;
using System.Text;

namespace IMODDesigns.Exceptions
{
    public class LockTableCreationFailedException : Exception
    {

        public LockTableCreationFailedException(string message) : base(message)
        {

        }
    }
}
