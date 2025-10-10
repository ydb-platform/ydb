#pragma once

#include <util/datetime/base.h>

#include <deque>
#include <set>
#include <unordered_set>

namespace NKikimr::NPQ::NMLP {

//
// На диске храним N типа блобов
// * Общая информация об процессе (мета), включающая FirstOffset, LastOffset, FirstUncommittedOffset и т.д. Всегда один блоб.
// * Информация о статусе обработке сообщений. MaxMessages * 4 байтов (~400Kb) . Всегда один блоб. Перезаписывается редко.
// * WAL изменений. Несколько блобов. Частая запись. Информация об измененных статусах сообщениях.

class TStorage {

    // Имеет смысл ограничить 100К сообщений на партицию. Надо больше - увеличивайте кол-во партиции
    // В худшем случае на 100000 сообщений надо ~5MB памяти
    static constexpr size_t MaxMessages = 100000;

    static constexpr size_t MaxDeadlineDelta = 1 << 16;
    static constexpr size_t MaxReleasedSize = 1024;

    enum EMessageStatus {
        Unprocessed = 0,
        Locked = 1,
        Committed = 2
    };

    struct TMessage {
        ui32 Status: 2; // Статус сообщения. EMessageStatus
        ui32 HasMessageGroupId: 1;
        ui32 DeadlineDelta: 15; // Для заблокированных
        ui32 Cookie: 13; // Cookie для заблокированных для исключения коммита из умерших сессий
        ui32 MessageGroupIdHash; // Hash группы сообщений (отбрасываем лишние биты)
    };

    struct LockedMessage {
         // Дельта от FirstOffset (надо будет обновлять все дельты при изменении FirstOffset)
        ui32 OffsetDelta : 17;
        // Дельта от BaseDeadline (надо будет обновлять все дельты при изменении BaseDeadline)
        // (какое максимальное значение timeout-а может быть? можем ли использовать меньше бит? например,
        // daedline не до миллисекунд, а до десчятых секунды т.к. чаще все равно обрабатываеть не будем.
        // если max timeout ограничить 1 минутой, то надо держать значение не больше 600 + частота обновление
        // BaseDeadline, например, раз в минуту, то макс значение 1200 - это 11 бит + 1 прозапас)
        // В SQS максимальный лимит 12 часов. В секундах это 43200, требуется 16 бит. 15 бит позволяет огграничить 
        // 32767 или 9 часов. (если не нужна кука, то можно сократить до 15 бит)
        ui32 DeadlineDelta : 15;

        LockedMessage(ui32 offsetDelta, ui32 deadlineDelta)
            : OffsetDelta(offsetDelta)
            , DeadlineDelta(deadlineDelta)
        {
        }
    };
public:

    struct TMessageId {
        ui64 Offset;
        ui64 Cookie;
    };

    // Return next message for client processing.
    // deadline - time for processing visibility
    // fromOffset indicates from which offset it is necessary to continue searching for the next free message.
    //            it is an optimization for the case when the method is called several times in a row.
    std::optional<TMessageId> Next(TInstant deadline, ui64 fromOffset = 0);
    bool Commit(TMessageId message);
    bool Unlock(TMessageId message);
    // For SQS compatibility
    // https://docs.amazonaws.cn/en_us/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html
    bool ChangeMessageDeadline(TMessageId message, TInstant deadline);

    bool ProccessDeadlines();

private:
    // offsetDelte, TMessage
    std::pair<ui64, TMessage*> GetMessage(ui64 offset, ui64 cookie, EMessageStatus expectedStatus);
    ui64 NormalizeDeadline(TInstant deadline);

    TMessageId DoLock(ui64 offsetDelta, TInstant deadline);
    bool DoCommit(ui64 offset, ui64 cookie);
    bool DoUnlock(ui64 offset, ui64 cookie);
    void DoUnlock(TMessage& message, ui64 offset);

    void UpdateDeltas(const ui64 newFirstOffset);
    void UpdateFirstUncommittedOffset();

private:
    // Первый загруженный оффсет  для обработки. Все сообщения с меньшим оффсетом либо уже закоммичены, либо удалены из партиции.
    // Как часто двигаем FirstOffset? Если FirstUncommittedOffset больше FirstOffset на 1000 (5000? 10000?) либо все сообщения обработаны закончились.
    ui64 FirstOffset;
    // Первый не закоммиченный оффсет для обработки. Всегда <= LastOffset
    ui64 FirstUncommittedOffset;
    // Первый не отданный клиенту для обработки. Всегда <= LastOffset
    ui64 FirstUnlockedOffset;


    // Список сообщений вычитанных ~400KB
    std::deque<TMessage> Messages;

    TInstant BaseDeadline;


    // Если timeout задается на сообщение. В SQS он задается на сообщение и по умолчанию равен 30 сек.
    // Список сообщений отданных клиенту. В худшем случае MaxMessages * 4 * {накладные расходы} ~ 4MB
    // Set упорядочен по (DeadlineDelta, OffsetDelta)
    std::set<LockedMessage> LockedMessages;

    // Список обрабатываемых MessageGroupId. Нельзя отдавать в обработку несколько сообщений с одной MessageGroup параллельно.
    std::unordered_set<ui32> LockedMessageGroupsId;

    // Список оффсетов сообщений, которые были отданы клиенту, но не были закоммичены на deadline
    // Храним здесь не более K сообщений (1000)
    // В первую очередь выдаем клиенту сообщения из этого списка. Если список пуст, то бежим по Messages
    // начиная от FirstUnlockedOffset и ищем сообщение, которое можно отдать для чтения (не забываем обновить
    // FirstUnlockedOffset, если не смогли поместить сообщение в ReleasedMessages).
    // В худшем случае список содержит 1000 * 16 = ~16Kb
    std::deque<ui64> ReleasedMessages;

    size_t LockCookie = 0;
};



}
