module PostgresTest

using Postgres
const pg = Postgres
using FactCheck

facts("Postgres Initialisation") do
	context("Setup") do
		options = Dict{ByteString, ByteString}()
		options["user"] = "postgres"

		conn = pg.pg_connect()
		msg = pg.pg_msg(:start, options)
		pg.writemsg(conn, msg)
		rmsg = pg.readmsg(conn)
		@fact pg.parsemsg(rmsg) => (:AuthenticationOK, )
		close(conn)
	end

	context("PGConnection") do
		conn = pg.PGConnection("postgres")
		close(conn)
	end
end
end
