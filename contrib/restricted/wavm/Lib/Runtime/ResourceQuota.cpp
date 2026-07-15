#include <memory>
#include "RuntimePrivate.h"
#include "WAVM/Platform/Mutex.h"
#include "WAVM/Runtime/Runtime.h"

using namespace WAVM;
using namespace WAVM::Runtime;

ResourceQuotaRef Runtime::createResourceQuota() { return std::make_shared<ResourceQuota>(); }

Uptr Runtime::getResourceQuotaMaxTableElems(ResourceQuotaConstRefParam resourceQuota)
{
	return resourceQuota->tableElems.getMax();
}

Uptr Runtime::getResourceQuotaCurrentTableElems(ResourceQuotaConstRefParam resourceQuota)
{
	return resourceQuota->tableElems.getCurrent();
}

void Runtime::setResourceQuotaMaxTableElems(ResourceQuotaRefParam resourceQuota, Uptr maxTableElems)
{
	resourceQuota->tableElems.setMax(maxTableElems);
}

Uptr Runtime::getResourceQuotaMaxMemoryPages(ResourceQuotaConstRefParam resourceQuota)
{
	return resourceQuota->memoryPages.getMax();
}

Uptr Runtime::getResourceQuotaCurrentMemoryPages(ResourceQuotaConstRefParam resourceQuota)
{
	return resourceQuota->memoryPages.getCurrent();
}

void Runtime::setResourceQuotaMaxMemoryPages(ResourceQuotaRefParam resourceQuota,
											 Uptr maxMemoryPages)
{
	resourceQuota->memoryPages.setMax(maxMemoryPages);
}
