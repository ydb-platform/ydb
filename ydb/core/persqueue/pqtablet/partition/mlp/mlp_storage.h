#pragma once

#include <util/datetime/base.h>

#include <deque>
#include <set>
#include <unordered_set>

namespace NKikimr::NPQ::NMLP {

//
// На диске храним 3 типа блобов
// * Общая информация об процессе (мета), включающая FirstUncommittedOffset и т.д. Всегда один блоб.
// * Информация о статусе обработке сообщений. MaxMessages * 8 байтов (~800Kb). Всегда один блоб. Перезаписывается редко.
// * WAL изменений. Несколько блобов. Частая запись. Информация об измененных статусах сообщениях.
class TStorage {

    // Имеет смысл ограничить 100К сообщений на партицию. Надо больше - увеличивайте кол-во партиции.
    // В худшем случае на 100000 сообщений надо ~5MB памяти
    static constexpr size_t MaxMessages = 100000;

    // Максимальное время блокировки сообщения (Message visibility timeout). Предлагаю ограничить 8 часами (в SQS 12 часов).
    static constexpr size_t MaxDeadlineDelta = 1 << 16;

    // Оптимизация. Кол-во сообщений, которые вернулись по timeout-у обратно для обработки, которые будут храниться в быстрой
    // зоне (их выборка будет происходить очень быстро, без поиска по списку всех Messages).
    static constexpr size_t MaxReleasedMessageSize = 1024;

    enum EMessageStatus {
        Unprocessed = 0,
        Locked = 1,
        Committed = 2
    };

    // sizeof(TMessage) == 8
    struct TMessage {
        // Статус сообщения. EMessageStatus
        ui32 Status: 2;
        ui32 HasMessageGroupId: 1;
        // Для заблокированных сообщений время, после которого сообщение должно вернуться в очередь.
        ui32 DeadlineDelta: 15;
        // Cookie для заблокированных для исключения коммита из умерших сессий. Если не нужно, то можно
        // уменьшить размер требуемой памяти на хранение сообщений в 2 раза (MessageGroupIdHash будем хранить только 14 бит)
        ui32 Cookie: 14;
        // Hash группы сообщений (храним hash т.к. нас устраивает вероятность блокироки разных групп - главно
        // не отдавать сообщения из одной группы параллельно)
        ui32 MessageGroupIdHash;
    };

    // sizeof(LockedMessage) == 4
    struct LockedMessage {
        // Дельта от FirstOffset (надо будет обновлять все дельты при изменении FirstOffset)
        // 0 <= OffsetDelta <= MaxMessages
        ui32 OffsetDelta : 17;
        // Дельта от BaseDeadline (надо будет обновлять все дельты при изменении BaseDeadline)
        // В секундах. В SQS задается в секундах, по умолчанию 30 сек, максимум 12 часов.
        // 0 << DeadlineDelta << MaxDeadlineDelta
        ui32 DeadlineDelta : 15;

        LockedMessage(ui32 offsetDelta, ui32 deadlineDelta)
            : OffsetDelta(offsetDelta)
            , DeadlineDelta(deadlineDelta)
        {
        }
    };

public:

    // Идентификатор сообщения.
    struct TMessageId {
        ui64 Offset;
        // Cookie используется для возможности исключить коммит из умерших сессии.
        // С другой стороны, ChangeMessageVisisbility в SQS можно вообще вызывать из консоли и там ничего не проверяется.
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

    // Список сообщений отданных клиенту.
    // В худшем случае MaxMessages * 4 * {накладные расходы хранения в map} ~ 2MB
    // Set упорядочен по (DeadlineDelta, OffsetDelta)
    std::set<LockedMessage> LockedMessages;

    // Список обрабатываемых MessageGroupId. Нельзя отдавать в обработку несколько сообщений с одной MessageGroup параллельно.
    // В худшем случае (все сообщений содержать разные message group id и все сообщения отданы клиенту) MaxMessages * 4 * {накладные расходы хранения в map} ~ 2MB
    std::unordered_set<ui32> LockedMessageGroupsId;

    // Список оффсетов сообщений, которые были отданы клиенту, но не были закоммичены на deadline
    // Храним здесь не более K сообщений (1000)
    // В первую очередь выдаем клиенту сообщения из этого списка. Если список пуст, то бежим по Messages
    // начиная от FirstUnlockedOffset и ищем сообщение, которое можно отдать для чтения.
    // В худшем случае список содержит 1000 * 16 = ~16Kb
    std::deque<ui64> ReleasedMessages;

    size_t LockCookie = 0;
};



}
