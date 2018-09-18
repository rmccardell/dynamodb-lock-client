using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;


namespace IMODDesigns.Utilities
{
    public class LockClientUtils
    {
        private static long NanoTime()
        {
            long nano = 10000L * Stopwatch.GetTimestamp();
            nano /= TimeSpan.TicksPerMillisecond;
            nano *= 100L;
            return nano;
        }


        public static long MillisecondTime()
        {
            //return (long) (NanoTime() * 0.000001);

           return (long) (new TimeSpan(DateTime.Now.Ticks)).TotalMilliseconds;
        }

        public static long TimeStamp()
        {
            long tickCount = System.Diagnostics.Stopwatch.GetTimestamp();
           // DateTime highResDateTime = new DateTime(tickCount);

            return (long) TimeSpan.FromTicks(tickCount).TotalMilliseconds;
        }



    }
}
