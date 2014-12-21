using Compat

import Base: length, write, read

const PROTOCOL_VERSION = uint32(0x0003_0000)
const CANCEL_CODE      = uint32(0x04d2_162e)
const SSL_CODE         = uint32(0x04d2_162f)

write_be(io, x) = write(io, hton(x))
read_be(io, T) = ntoh(read(io, T))

immutable MSG{I}
  data :: IOBuffer

  MSG() = new(PipeBuffer())
  MSG(data :: IOBuffer) = new(data)
end

# Definition taken from
# https://github.com/JuliaLang/julia/blob/04893a165aed19d898a9ed2f9dc2202553906256/base/iobuffer.jl#L106
function length(msg :: MSG)
  io = data(msg)
  (io.seekable ? io.size : nb_available(io))
end
data(msg :: MSG) = msg.data
ident{I}(:: MSG{I}) = I

write(m :: MSG, x) = write(data(m), x)
read(m :: MSG, x) = read(data(m), x)

pg_msg(i :: Symbol, args...) = pg_msg(MSG{i}(), args...)
pg_msg(i :: MSG, args...) = error("Could not construct message for ident: $(ident(i))")

function pg_msg(msg :: MSG{:start}, options :: Dict{ByteString, ByteString}, version = PROTOCOL_VERSION)
  write_be(msg, version)

  for (key, value) in options
    write(msg, key)
    write(msg, '\0')
    write(msg, value)
    write(msg, '\0')
  end

  write(msg, '\0')
  msg
end


function writemsg(conn, msg :: MSG)
  out = PipeBuffer()
  id = ident(msg)
  if id != :start
    write(out, id)
  end

  write_be(out, int32(length(msg) + sizeof(Int32))) # Length - big endian
  write(out, data(msg))
  write(conn, takebuf_array(out))
end
