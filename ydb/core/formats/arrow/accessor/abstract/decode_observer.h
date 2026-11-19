#pragma once

#include <util/system/types.h>

#include <memory>
#include <optional>

namespace NKikimr::NArrow {

    namespace NAccessor {

        class TColumnLoader;

        class IDecodeScope {
        public:
            virtual ~IDecodeScope() = default;
            virtual void Finish(std::optional<ui64> outputRows) = 0;
        };

        class IDecodeObserver {
        public:
            virtual ~IDecodeObserver() = default;
            virtual std::unique_ptr<IDecodeScope> OnDecodeBegin(
                const TColumnLoader& loader, ui64 encodedBytes, std::optional<ui64> rows) = 0;
        };

    } // namespace NAccessor
} // namespace NKikimr::NArrow
