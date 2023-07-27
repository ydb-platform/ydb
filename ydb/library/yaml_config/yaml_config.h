#pragma once

#include <library/cpp/yaml/fyamlcpp/fyamlcpp.h>
#include <library/cpp/actors/core/actor.h>

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/library/yaml_config/public/yaml_config.h>

#include <openssl/sha.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/set.h>
#include <util/generic/map.h>
#include <util/stream/str.h>

#include <unordered_map>
#include <map>
#include <string>

namespace NYamlConfig {

/**
 * Converts YAML representation to ProtoBuf
 */
NKikimrConfig::TAppConfig YamlToProto(const NFyaml::TNodeRef& node, bool allowUnknown = false, bool preTransform = true);

/**
 * Resolves config for given labels and stores result to appConfig
 * Stores intermediate resolve data in resolvedYamlConfig and resolvedJsonConfig if given
 */
void ResolveAndParseYamlConfig(
    const TString& yamlConfig,
    const TMap<ui64, TString>& volatileYamlConfigs,
    const TMap<TString, TString>& labels,
    NKikimrConfig::TAppConfig& appConfig,
    TString* resolvedYamlConfig = nullptr,
    TString* resolvedJsonConfig = nullptr);

/**
 * Replaces kinds not managed by yaml config (e.g. NetClassifierConfig) from config 'from' in config 'to'
 * if corresponding configs are presenet in 'from'
 */
void ReplaceUnmanagedKinds(const NKikimrConfig::TAppConfig& from, NKikimrConfig::TAppConfig& to);

} // namespace NYamlConfig
