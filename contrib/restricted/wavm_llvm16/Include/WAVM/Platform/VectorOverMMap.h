#include <sys/mman.h>
#include <cstddef>

namespace WAVM { namespace Runtime {

	class VectorOverMMap
	{
	public:
		VectorOverMMap();
		~VectorOverMMap();

		VectorOverMMap(const VectorOverMMap& other) = delete;
		VectorOverMMap& operator=(const VectorOverMMap& other) = delete;

		void grow(size_t morePages);

		static constexpr size_t getNumGuardBytes() { return guardPageCount * pageSize; }

		size_t getNumReservedBytes() const;

		void* getData() const;

	private:
		void resizeWithDoubling(size_t morePages);

		static void* allocateAndProtect(size_t committedPageCount, size_t capacityPageCount);

		static void checkForOOM(const char* message);

		static constexpr size_t pageSize = 65536;
		static constexpr size_t guardPageCount = 2;

		size_t committedPageCount = 0;
		size_t capacityPageCount = 0;
		void* data = nullptr;
	};

}}
