import Base: AsyncStream, write, close

const DEFAULT_PORT = 5432

immutable PGConnection
  underlying :: AsyncStream

  parameterStatus :: Dict{String, String}

  processID :: Nullable{Int32}
  backendKey :: Nullable{Int32}
end

write(c :: PGConnection, x) = write(stream(c), x)
close(c :: PGConnection) = close(stream(c))
isopen(c :: PGConnection) = isopen(stream(c))

stream(c :: PGConnection) = c.underlying

function PGConnection(user :: ByteString, host = "localhost",
                      port = DEFAULT_PORT; require_ssl=false)
  options = Dict{ByteString, ByteString}()
  options["user"] = user

  # Create connection
  conn = pg_connect(host, port, require_ssl)

  # Send startup msg
  writemsg(conn, pg_msg(:start, options))

  pStatus = Dict{ByteString, ByteString}()
  pID = Nullable{Int32}()
  secret = Nullable{Int32}()

  msg = parsemsg(readmsg(conn))
  id = msg[1]
  while id != :ReadyForQuery
    if id == :ParameterStatus
      pStatus[msg[2]] = msg[3]
    elseif id == :BackendKeyData
      pID = Nullable(msg[2])
      secret = Nullable(msg[3])
    end

    msg = parsemsg(readmsg(conn))
    id = msg[1]
  end

  PGConnection(conn, pStatus, pID, secret)
end


function pg_connect(host = "localhost", port = DEFAULT_PORT, require_ssl=false)
  base_conn = connect(host, port)

  #Check for ssl
  sslmsg = pg_msg(:start, Dict{ByteString, ByteString}(), SSL_CODE)
  writemsg(base_conn, sslmsg)

  response = read(base_conn, Char) # Possible results 'N', 'Y' and 'E'

  if response == 'N'
    info("Connection $(host):$(port) does not support SSL")
    require_ssl && error("SSL required for connection. $(host):$(port)")
    return base_conn # Elevate unsecure connection to prinicipal connection
  elseif response == 'S'
    sess = GnuTLS.Session()
    set_priority_string!(sess)
    set_credentials!(sess,GnuTLS.CertificateStore())
    associate_stream(sess,base_conn)
    handshake!(sess)

    debug("SSL connection successfully initialised")
    return sess # Use secure connection
  elseif response == 'E'
    # Code duplication from message.jl
    l = read_be(conn, Int32) - sizeof(Int32)
    data = IOBuffer(read(conn, UInt8, l))
    DBError(MSG{:E}(data))
  else
    error("Nonsense: $response in pg_connect")
  end
end

