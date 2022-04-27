using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Collections.Generic;

namespace ZeroMQ
{
    public class UBuffer
    {
        public const int DefaultFrameSize = ZFrame.DefaultFrameSize;

        public int Position;
        //private DispoIntPtr framePtr;

        public readonly byte[] Buffer;
        public UnmanagedMemoryStream MemStream;

        public UBuffer()
            : this(DefaultFrameSize)
        {
        }

        public UBuffer(int capacity)
        {
            Position = 0;
            Buffer = new byte[capacity];
        }

        public int Length
        {
            get
            {
                return Buffer.Length;
            }
        }
    }
}
