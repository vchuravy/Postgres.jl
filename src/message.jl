import Base: length, write, read, readall, readuntil, readbytes

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
read(m :: MSG, x, l) = read(data(m), x, l)
readall(m :: MSG) = readall(data(m))
readuntil(m :: MSG, c) = readuntil(data(m), c)
readbytes(m :: MSG) = readbytes(data(m))

readstring(m :: MSG) = readuntil(m, '\0')[1:end-1]

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
    return (:AuthenticationOK, )
  elseif request == 2
    return (:AuthenticationKerberosV5, )
  elseif request == 3
    return (:AuthenticationClearTextPassword, )
  elseif request == 5
    salt = read(msg, UInt8, 4)
    return (:AuthenticationMD5Password, salt)
  elseif request == 6
    return (:AuthenticationSCMCredential, )
  elseif request == 7
    return (:AuthenticationGSS, )
  elseif request == 9
    return (:AuthenticationSSPI, )
  elseif request == 8
    data = read(msg, UInt8, length(msg))
    return (:AuthenticationGSSContinue, data)
  else
    error("Does not support AuthenticationRequest $request")
  end
end

function parsemsg(msg :: MSG{:K})
  id  = read_be(msg, Int32)
  key = read_be(msg, Int32)
  :BackendKeyData, id, key
end

#TODO does this work
parsemsg(:: MSG{symbol('2')}) = (:BindComplete, )

#TODO does this work
parsemsg(:: MSG{symbol('3')}) = (:CloseComplete, )

parsemsg(msg :: MSG{:C}) = (:CommandComplete, readstring(msg))

function parsemsg(msg :: MSG{:d})
  payload = read(msg, UInt8, length(msg))

  :CopyData, payload
end

parsemsg(msg :: MSG{:c}) = (:CopyDone, )

function parsemsg(msg :: MSG{:G})
  format = read_be(msg, Int8) == 0 ? :textual : :binary
  n = read_be(msg, Int16)
  columns = Array(Int16, n)
  for i in 1:n
    columns[i] = read_be(msg, Int16)
  end

  :CopyInResponse, format, columns
end

function parsemsg(msg :: MSG{:H})
  format = read_be(msg, Int8) == 0 ? :textual : :binary
  n = read_be(msg, Int16)
  columns = Array(Int16, n)
  for i in 1:n
    columns[i] = read_be(msg, Int16)
  end

  :CopyOutResponse, format, columns
end

function parsemsg(msg :: MSG{:W})
  format = read_be(msg, Int8) == 0 ? :textual : :binary
  n = read_be(msg, Int16)
  columns = Array(Int16, n)
  for i in 1:n
    columns[i] = read_be(msg, Int16)
  end

  :CopyBothResponse, format, columns
end

function parsemsg(msg :: MSG{:D})
  n = read_be(msg, Int16)
  columns = Array(Nullable{Vector{UInt8}}, n)

  for i in 1:n
    c = read_be(msg, Int32)

    if c == -1
      val = Nullable{Vector{UInt8}}()
    else
      val = Nullable(read(msg, UInt8, c))
    end

    columns[i] = val
  end

  :DataRow, columns
end

parsemsg(msg :: MSG{:I}) = (:EmptyQueryResponse, )

parsemsg(msg :: MSG{:E}) = (:ErrorResponse, DBError(msg))

function parsemsg(msg :: MSG{:V})
  lr = read_be(msg, Int32)
  if lr == -1
    val = Nullable{Vector{UInt8}}()
  else
    val = Nullable(read(msg, UInt8, lr))
  end

  :FunctionCallResponse, val
end

parsemsg(msg :: MSG{:n}) = (:NoData, )

parsemsg(msg :: MSG{:N}) = (:NoticeResponse, DBError(msg))

function parsemsg(msg :: MSG{:A})
  id = read_be(msg, Int32)
  name = readstring(msg)
  payload = readstring(msg)

  :NotificationResponse, id, name, payload
end

function parsemsg(msg :: MSG{:t})
  n = read_be(msg, Int16)
  params = Array(Int32, n)
  for i in 1:n
    params[i] = read_be(msg, Int32)
  end

  :ParameterDescription, params
end

function parsemsg(msg :: MSG{:S})
  name = readstring(msg)
  value = readstring(msg)

  :ParameterStatus, name, value
end

#TODO: Does this work
parsemsg(msg :: MSG{symbol('1')}) = (:ParseComplete, )

parsemsg(msg :: MSG{:s}) = :PortalSuspended

function parsemsg(msg :: MSG{:Z})
  status = read(msg, Char)
  :ReadyForQuery, status
end

function parsemsg(msg :: MSG{:B})
  n = read_be(msg, Int16)
  fields = Array{Field, n}

  for i in 1:n
    name          = readstring(msg)
    tableID       = read_be(msg, Int32)
    columnID      = read_be(msg, Int16)
    datatypeID    = read_be(msg, Int32)
    datatypeSize  = read_be(msg, Int16)
    typeModifier  = read_be(msg, Int32)
    format        = read_be(msg, Int16) == 0 ? :textual : :binary

    fields[i] = Field{format}(name, tableID, columnID, datatypeID,
                              datatypeSize, typeModifier)
  end

  :RowDescription, fields
end
