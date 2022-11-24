using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Collections.Generic;

using ZeroMQ.lib;

namespace ZeroMQ
{
    public class UFrameBuffer
    {
        public const int DefaultFrameSize = ZFrame.DefaultFrameSize;

        public int Position;
        //private DispoIntPtr framePtr;

        public byte[] Header;
        public byte[] Buffer;

        // Class System.Buffer "manipulates arrays of primitive types"

        protected static readonly System.Buffers.ArrayPool<byte> tinyArrays = System.Buffers.ArrayPool<byte>.Create(128, 256);
        protected static readonly System.Buffers.ArrayPool<byte> largeArrays = System.Buffers.ArrayPool<byte>.Create(65536, 16);

        public UFrameBuffer()
            : this(DefaultFrameSize)
        {
        }

        public UFrameBuffer(int capacity)
        {
            Position = 0;
            Header = tinyArrays.Rent(zmq.sizeof_zmq_msg_t);
            Buffer = largeArrays.Rent(capacity);


        }

        internal static DispoIntPtr CreateEmptyNative()
        {
            var msg = DispoIntPtr.Alloc(zmq.sizeof_zmq_msg_t);

            ZError error;
            while (-1 == zmq.msg_init(msg))
            {
                error = ZError.GetLastErr();

                if (error == ZError.EINTR)
                {
                    error = default(ZError);
                    continue;
                }

                msg.Dispose();

                throw new ZException(error, "zmq_msg_init");
            }

            return msg;
        }

        internal static DispoIntPtr CreateNative(int size)
        {
            var msg = DispoIntPtr.Alloc(zmq.sizeof_zmq_msg_t);

            ZError error;
            while (-1 == zmq.msg_init_size(msg, size))
            {
                error = ZError.GetLastErr();

                if (error == ZError.EINTR)
                {
                    error = default(ZError);
                    continue;
                }

                msg.Dispose();

                if (error == ZError.ENOMEM)
                {
                    throw new OutOfMemoryException("zmq_msg_init_size");
                }
                throw new ZException(error, "zmq_msg_init_size");
            }
            return msg;
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
