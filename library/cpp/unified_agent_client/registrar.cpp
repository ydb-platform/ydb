#include "backend_creator.h"

namespace NUnifiedAgent {

    ILogBackendCreator::TFactory::TRegistrator<NUnifiedAgent::TLogBackendCreator> TLogBackendCreator::Registrar("unified_agent");

}

