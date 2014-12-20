using Compat

import Base: length

const PROTOCOL_VERSION = uint32(0x0003_0000)
const CANCEL_CODE      = uint32(0x04d2_162e)
const SSL_CODE         = uint32(0x04d2_162f)

write_be(conn, x) = write(conn, hton(x))

immutable MSG{I}
  data :: Vector{UInt8}

  MSG() = new(Array(UInt8,0))
  MSG(data) = new(data)
end

MSG{I}( :: MSG{I}, data) = MSG{I}(data)

length(msg :: MSG) = length(data(msg))
data(msg :: MSG) = msg.data
ident{I}(:: MSG{I}) = I

pg_msg(i :: Symbol, args...) = pg_msg(MSG{i}(), args...)
pg_msg(i :: MSG, args...) = error("Could not construct message for ident: $(ident(i))")

function pg_msg(msg :: MSG{:start}, options :: Dict{ByteString, ByteString}, version = PROTOCOL_VERSION)
  out = PipeBuffer()

  write_be(out, version)

  for (key, value) in options
    write(out, key)
    write(out, '\0')
    write(out, value)
    write(out, '\0')
  end

  write(out, '\0')
  MSG(msg, takebuf_array(out))
end


function writemsg(conn, msg :: MSG)
  out = PipeBuffer()
  if ident(msg) != :start
    # Write ident char
  end

  write_be(out, int32(length(msg) + sizeof(Int32))) # Length - big endian
  write(out, data(msg))
  write(conn, takebuf_array(out))
end
