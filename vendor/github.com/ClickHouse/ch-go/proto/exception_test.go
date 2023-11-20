package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestException_Encode(t *testing.T) {
	e := &Exception{
		Code:    ErrUnknownTable,
		Name:    "DB::Exception",
		Message: "DB::Exception: Table default._3_ doesn't exist",
		Stack: "0. DB::Exception::Exception(std::__1::basic_string<char, " +
			"std::__1::char_traits<char>, std::__1::allocator<char> > const" +
			"&, int, bool) @ 0x9b722d4 in /usr/bin/clickhouse\n1. DB::Excep" +
			"tion::Exception<std::__1::basic_string<char, std::__1::char_tr" +
			"aits<char>, std::__1::allocator<char> > >(int, std::__1::basic" +
			"_string<char, std::__1::char_traits<char>, std::__1::allocator" +
			"<char> > const&, std::__1::basic_string<char, std::__1::char_t" +
			"raits<char>, std::__1::allocator<char> >&&) @ 0x9c52143 in /us" +
			"r/bin/clickhouse\n2. void std::__1::__optional_storage_base<DB" +
			"::Exception, false>::__construct<int const&, char const (&) [2" +
			"3], std::__1::basic_string<char, std::__1::char_traits<char>, " +
			"std::__1::allocator<char> > >(int const&, char const (&) [23]," +
			" std::__1::basic_string<char, std::__1::char_traits<char>, std" +
			"::__1::allocator<char> >&&) @ 0x11f4fa0f in /usr/bin/clickhous" +
			"e\n3. DB::DatabaseCatalog::getTableImpl(DB::StorageID const&, " +
			"std::__1::shared_ptr<DB::Context const>, std::__1::optional<DB" +
			"::Exception>*) const @ 0x11f405c6 in /usr/bin/clickhouse\n4. D" +
			"B::DatabaseCatalog::getDatabaseAndTable(DB::StorageID const&, " +
			"std::__1::shared_ptr<DB::Context const>) const @ 0x11f47503 in" +
			" /usr/bin/clickhouse\n5. DB::InterpreterDropQuery::executeToTa" +
			"bleImpl(DB::ASTDropQuery&, std::__1::shared_ptr<DB::IDatabase>" +
			"&, StrongTypedef<wide::integer<128ul, unsigned int>, DB::UUIDT" +
			"ag>&) @ 0x122ccacb in /usr/bin/clickhouse\n6. DB::InterpreterD" +
			"ropQuery::executeToTable(DB::ASTDropQuery&) @ 0x122cc106 in /u" +
			"sr/bin/clickhouse\n7. DB::InterpreterDropQuery::execute() @ 0x" +
			"122cbc08 in /usr/bin/clickhouse\n8. ? @ 0x12788949 in /usr/bin" +
			"/clickhouse\n9. DB::executeQuery(std::__1::basic_string<char, " +
			"std::__1::char_traits<char>, std::__1::allocator<char> > const" +
			"&, std::__1::shared_ptr<DB::Context>, bool, DB::QueryProcessin" +
			"gStage::Enum) @ 0x127868d3 in /usr/bin/clickhouse\n10. DB::TCP" +
			"Handler::runImpl() @ 0x13118570 in /usr/bin/clickhouse\n11. DB" +
			"::TCPHandler::run() @ 0x1312c019 in /usr/bin/clickhouse\n12. P" +
			"oco::Net::TCPServerConnection::start() @ 0x15d729af in /usr/bi" +
			"n/clickhouse\n13. Poco::Net::TCPServerDispatcher::run() @ 0x15" +
			"d74da1 in /usr/bin/clickhouse\n14. Poco::PooledThread::run() @" +
			" 0x15e89749 in /usr/bin/clickhouse\n15. Poco::ThreadImpl::runn" +
			"ableEntry(void*) @ 0x15e86e80 in /usr/bin/clickhouse\n16x\n17." +
			" /build/glibc-YbNSs7/glibc-2.31/misc/../sysdeps/unix/sysv/linu" +
			"x/x86_64/clone.S:97: clone @ 0x122293 in /usr/lib/debug/lib/x8" +
			"6_64-linux-gnu/libc-2.31.so\n",
	}
	Gold(t, e)

	t.Run("Decode", func(t *testing.T) {
		var b Buffer
		e.EncodeAware(&b, Version)

		dec := &Exception{}
		requireDecode(t, b.Buf, aware(dec))
		require.Equal(t, e, dec)
		requireNoShortRead(t, b.Buf, aware(dec))
	})
}
