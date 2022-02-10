#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <util/datetime/base.h>

namespace NKikimr::NDataStreams::V1 {

  class TNextToken {
  public:
      static constexpr ui32 LIFETIME_MS = 300*1000;

      TNextToken(const TString& nextToken)
          : Valid{true}
      {
          try {
              TString decoded;
              Base64Decode(nextToken, decoded);
              auto ok = Proto.ParseFromString(decoded);
              if (ok) {
                  Valid = IsAlive(TInstant::Now().MilliSeconds());
              } else {
                  Valid = false;
              }
          } catch (std::exception&) {
              Valid = false;
          }
      }

      TNextToken(const TString& streamArn, ui32 alreadyRead, ui32 maxResults, ui64 creationTimestamp)
          : Valid{true}
      {
          Proto.SetStreamArn(streamArn);
          Proto.SetAlreadyRead(alreadyRead);
          Proto.SetMaxResults(maxResults);
          Proto.SetCreationTimestamp(creationTimestamp);
      }

      TString Serialize() const {
          TString data;
          bool result = Proto.SerializeToString(&data);
          Y_VERIFY(result);
          TString encoded;
          Base64Encode(data, encoded);
          return encoded;
      }

      ui32 GetAlreadyRead() const {
          return Proto.GetAlreadyRead();
      }

      TString GetStreamArn() const {
          return Proto.GetStreamArn();
      }

      TString GetStreamName() const {
          return Proto.GetStreamArn();
      }

      ui32 GetMaxResults() const {
          return Proto.GetMaxResults();
      }

      ui32 GetCreationTimestamp() const {
          return Proto.GetCreationTimestamp();
      }

      bool IsAlive(ui64 now) const {
          return now >= Proto.GetCreationTimestamp() &&
              (now - Proto.GetCreationTimestamp()) < LIFETIME_MS;
      }

      bool IsValid() const {
          return Valid && Proto.GetStreamArn().size() > 0;
      }

  private:
      bool Valid;
      NKikimrPQ::TYdsNextToken Proto;
  };

} // namespace NKikimr::NDataStreams::V1
