module PostgresTest

using Postgres
using FactCheck

facts("Postgres Initialisation") do
	context("Setup") do
		options = Dict{ByteString, ByteString}()
		options["user"] = "postgres"

		conn = pg_connect()
		msg = Postgres.pg_msg(:start, options)
		Postgres.writemsg(conn, msg)
	end
end
end
