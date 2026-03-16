#pragma once

#include <util/system/types.h>

namespace NKikimr::NKqp {

// Used only to count channels for a single query.

class TNodeQuotaManager {
public:
    TNodeQuotaManager(ui64 maxChannelBuffersSize, ui64 minChannelBufferSize);

    void Reset();

    // StageId одной таски
    // NodeId всех тасок
    // Сколько всего тасок
    // Связи с предыдущими Stage
    // Масштабируемость (true/false) - пока что будем урезать только таски, у которых input = shuffle.

    // Add from graph lists to root - topologically sorted stages.
    void AddTasks(ui64 nodeId, ui64 count, bool shrinkable);

    size_t GetStageTasks() const;
};

} // namespace NKikimr::NKqp
