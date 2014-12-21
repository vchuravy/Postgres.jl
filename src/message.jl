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

function readmsg(conn)
  ident = symbol(read(conn, Char))
  length = read_be(conn, Int32) - sizeof(Int32)
  data = IOBuffer(read(conn, UInt8, length))
  MSG{ident}(data)
end

##
# Function: parsemsg
# Parses a postgres Backend message after:
# http://www.postgresql.org/docs/9.4/static/protocol-message-formats.html
#
# Each message is identified by a ident char
##
function parsemsg(msg :: MSG)
  error("Can't parse postgres message with ident $(ident(msg))")
end

function parsemsg(msg :: MSG{:R})
    request = read_be(msg, Int32)

    if request == 0
      return :AuthenticationOK
    else
      error("Does not support this AuthenticationRequest $request")
    end
end

function parsemsg(msg :: MSG{:K})
  error("Can't parse BackendKeyData msg")
end

#TODO does this work
function parsemsg(msg :: MSG{symbol('2')})
  error("Can't parse BindComplete msg")
end

#TODO does this work
function parsemsg(msg :: MSG{symbol('3')})
  error("Can't parse CloseComplete msg")
end

function parsemsg(msg :: MSG{:C})
  error("Can't parse CommandComplete msg")
end

function parsemsg(msg :: MSG{:d})
  error("Can't parse CopyData msg")
end

function parsemsg(msg :: MSG{:c})
  error("Can't parse CopyDone msg")
end

function parsemsg(msg :: MSG{:G})
  error("Can't parse CopyInResponse msg")
end

function parsemsg(msg :: MSG{:H})
  error("Can't parse CopyOutResponse msg")
end

function parsemsg(msg :: MSG{:W})
  error("Can't parse CopyBothResponse msg")
end

function parsemsg(msg :: MSG{:D})
  error("Can't parse DataRow msg")
end

function parsemsg(msg :: MSG{:I})
  error("Can't parse EmptyQueryResponse msg")
end

function parsemsg(msg :: MSG{:E})
  error("Can't parse Error msg")
end

function parsemsg(msg :: MSG{:V})
  error("Can't parse FunctionCallResponse msg")
end

function parsemsg(msg :: MSG{:n})
  error("Can't parse NoData msg")
end

function parsemsg(msg :: MSG{:N})
  error("Can't parse NoticeResponse msg")
end

function parsemsg(msg :: MSG{:A})
  error("Can't parse NotificationResponse msg")
end

function parsemsg(msg :: MSG{:t})
  error("Can't parse ParameterDescription msg")
end

function parsemsg(msg :: MSG{:S})
  error("Can't parse ParameterStatus msg")
end

#TODO: Does this work
function parsemsg(msg :: MSG{symbol('1')})
  error("Can't parse ParseComplete msg")
end

function parsemsg(msg :: MSG{:s})
  error("Can't parse PortalSuspended msg")
end

function parsemsg(msg :: MSG{:Z})
  error("Can't parse ReadyForQuery msg")
end

function parsemsg(msg :: MSG{:B})
  error("Can't parse RowDescription msg")
end
