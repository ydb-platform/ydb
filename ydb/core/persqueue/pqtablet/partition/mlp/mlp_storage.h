#pragma once

#include "mlp.h"

#include <util/datetime/base.h>

#include <deque>
#include <unordered_set>

namespace NKikimr::NPQ::NMLP {

class TSerializer;    

//
// На диске храним 3 типа блобов
// * Общая информация об процессе (мета), включающая FirstUncommittedOffset и т.д. Всегда один блоб.
// * Информация о статусе обработке сообщений. MaxMessages * 8 байтов (~800Kb). Всегда один блоб. Перезаписывается редко.
// * WAL изменений. Несколько блобов. Частая запись. Информация об измененных статусах сообщениях.
class TStorage {

    friend class TSerializer;

    // Имеет смысл ограничить 100К сообщений на партицию. Надо больше - увеличивайте кол-во партиции.
    // В худшем случае на 100000 сообщений надо ~3MB памяти
    static constexpr size_t MaxMessages = 100000;

    // Максимальное время блокировки сообщения (Message visibility timeout). (в SQS 12 часов).
    static constexpr size_t MaxDeadlineDelta = Max<ui16>();

    // Оптимизация. Кол-во сообщений, которые вернулись по timeout-у обратно для обработки, которые будут храниться в быстрой
    // зоне (их выборка будет происходить очень быстро, без поиска по списку всех Messages).
    static constexpr size_t MaxReleasedMessageSize = 1024;

    enum EMessageStatus {
        Unprocessed = 0,
        Locked = 1,
        Committed = 2,
        DLQ = 3
    };

    struct TMessage {
        // Статус сообщения. EMessageStatus
        ui64 Status: 3 = EMessageStatus::Unprocessed;
        ui64 Reserve: 2;
        ui64 HasMessageGroupId: 1 = false;
        // Сколько раз отдавали сообщение для чтения. В SQS максимальное значение 1000 (интернеты про это пишут, но в документации не нашел)
        ui64 ReceiveCount: 10 = 0;
        // Для заблокированных сообщений время, после которого сообщение должно вернуться в очередь.
        ui64 DeadlineDelta: 16 = 0;
        // Hash группы сообщений (храним hash т.к. нас устраивает вероятность блокироки разных групп - главно
        // не отдавать сообщения из одной группы параллельно)
        ui64 MessageGroupIdHash: 32 = 0;
    };

    static_assert(sizeof(TMessage) == sizeof(ui64));

public:

    // Return next message for client processing.
    // deadline - time for processing visibility
    // fromOffset indicates from which offset it is necessary to continue searching for the next free message.
    //            it is an optimization for the case when the method is called several times in a row.
    struct NextResult {
        TMessageId Message;
        ui64 FromOffset;
    };
    std::optional<NextResult> Next(TInstant deadline, ui64 fromOffset = 0);
    bool Commit(TMessageId message);
    bool Unlock(TMessageId message);
    // For SQS compatibility
    // https://docs.amazonaws.cn/en_us/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html
    bool ChangeMessageDeadline(TMessageId message, TInstant deadline);

    bool ProccessDeadlines();
    bool Compact();

    bool InitializeFromSnapshot(const NKikimrPQ::TMLPStorageSnapshot& snapshot);
    bool CreateSnapshot(NKikimrPQ::TMLPStorageSnapshot& snapshot);

private:
    // offsetDelte, TMessage
    std::pair<ui64, TMessage*> GetMessage(ui64 offset, EMessageStatus expectedStatus);
    ui64 NormalizeDeadline(TInstant deadline);

    TMessageId DoLock(ui64 offsetDelta, TInstant deadline);
    bool DoCommit(ui64 offset);
    bool DoUnlock(ui64 offset);
    void DoUnlock(TMessage& message, ui64 offset);

    void UpdateDeltas();
    void UpdateFirstUncommittedOffset();

private:
    // Первый загруженный оффсет  для обработки. Все сообщения с меньшим оффсетом либо уже закоммичены, либо удалены из партиции.
    // Как часто двигаем FirstOffset? Когда сохраняем большой блоб. 
    // Как часто сохраняем большой блоб? Если FirstUncommittedOffset больше FirstOffset на 1000 (5000? 10000?) либо все сообщения обработаны и закончились / раз в N секунд.
    ui64 FirstOffset;
    // Первый не закоммиченный оффсет для обработки. Всегда <= LastOffset
    ui64 FirstUncommittedOffset;
    // Первый не отданный клиенту для обработки. Всегда <= LastOffset
    ui64 FirstUnlockedOffset;
    // Время, от которого отсчитываются delta deadline-ов. Позволяет использовать не ui64, а 15 бит.
    // Недостаток - периодически надо смещать BaseDeadline и пересчитывать текущие дельты.
    // Если клиент не использует большие visisbility timeouts, то пересчет редкий (раз в несколько часов)
    TInstant BaseDeadline;

    // Список сообщений вычитанных (при 100000 сообщений ~800KB)
    // Максимум храним в памяти MaxMessages сообщений, но для небольших очередей кол-во сообщений ограничиваем 1000.
    // Для больших очередей стараемся размер подобрать оптимально, например, в зависимости от одновременно обрабатываемых
    // клиентом сообщений (умножаем их на 2)
    std::deque<TMessage> Messages;

    // Список обрабатываемых MessageGroupId. Нельзя отдавать в обработку несколько сообщений с одной MessageGroup параллельно.
    // В худшем случае (все сообщений содержать разные message group id и все сообщения отданы клиенту) MaxMessages * 4 * {накладные расходы хранения в map} ~ 2MB
    std::unordered_set<ui32> LockedMessageGroupsId;

    // Список оффсетов сообщений, которые были отданы клиенту, но не были закоммичены на deadline
    // Храним здесь не более K сообщений (1000)
    // В первую очередь выдаем клиенту сообщения из этого списка. Если список пуст, то бежим по Messages
    // начиная от FirstUnlockedOffset и ищем сообщение, которое можно отдать для чтения.
    // В худшем случае список содержит 1000 * 16 = ~16Kb
    std::deque<ui64> ReleasedMessages;
};



}
