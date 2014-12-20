using GnuTLS

const DEFAULT_PORT = 5432

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
    error("Don't know how to handle error")
    #todo handle error
  else
    error("Nonsense: $response")
    # todo nonsense
  end
end

