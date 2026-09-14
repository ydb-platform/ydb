#pragma once

#include <string>
#include "WAVM/Logging/Logging.h"

namespace WAVM { namespace IR {
	struct Module;
	struct FeatureSpec;
}};

int execAssembleCommand(int argc, char** argv);
int execDisassembleCommand(int argc, char** argv);
int execTestCommand(int argc, char** argv);
int execVersionCommand(int argc, char** argv);

void showAssembleHelp(WAVM::Log::Category outputCategory);
void showDisassembleHelp(WAVM::Log::Category outputCategory);
void showTestHelp(WAVM::Log::Category outputCategory);
void showVersionHelp(WAVM::Log::Category outputCategory);

#if WAVM_ENABLE_RUNTIME
int execCompileCommand(int argc, char** argv);
int execRunCommand(int argc, char** argv);

void showCompileHelp(WAVM::Log::Category outputCategory);
void showRunHelp(WAVM::Log::Category outputCategory);
#endif

std::string getFeatureListHelpText();
bool parseAndSetFeature(const char* featureName, WAVM::IR::FeatureSpec& featureSpec, bool enable);
