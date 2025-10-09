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
// * Deadlins обрабатываемых сообщений. Один блоб. Перезаписывается редко. Если maxdeadline < 16 sec и все MaxMessages
//   сообщения отданы, то размер MaxMessages байт (по одному байту на deadline delta, упаковываем дельтой от предыдущего значения) 100Kb + MaxMessages * 2 (2 байта на delta offset от FirstOffset) 200Kb (итого 300Kb)
// * WAL deadlines. Несколько блобов. Частая запись. (TODO объединить с WAL статусов)
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
        ui32 MessageGroupIdHash: 13; // Hash группы сообщений (отбрасываем лишние биты)
        ui32 DeadlineDelta: 16; // Для заблокированных
    };

    struct LockedMessage {
         // Дельта от FirstOffset (надо будет обновлять все дельты при изменении FirstOffset)
        ui32 OffsetDelta : 17;
        // Защита от коммита от протухшего клиента (нужна?)
        ui32 Cookie0: 15;
        ui32 Cookie1: 16;
        // Дельта от BaseDeadline (надо будет обновлять все дельты при изменении BaseDeadline)
        // (какое максимальное значение timeout-а может быть? можем ли использовать меньше бит? например,
        // daedline не до миллисекунд, а до десчятых секунды т.к. чаще все равно обрабатываеть не будем.
        // если max timeout ограничить 1 минутой, то надо держать значение не больше 600 + частота обновление
        // BaseDeadline, например, раз в минуту, то макс значение 1200 - это 11 бит + 1 прозапас)
        // В SQS максимальный лимит 12 часов. В секундах это 43200, требуется 16 бит. 15 бит позволяет огграничить 
        // 32767 или 9 часов. (если не нужна кука, то можно сократить до 15 бит)
        ui32 DeadlineDelta : 16;

        LockedMessage(ui32 offsetDelta, ui32 deadlineDelta, ui32 cookie)
            : OffsetDelta(offsetDelta)
            , Cookie0(cookie & 0x7FFF)
            , Cookie1(cookie >> 15)
            , DeadlineDelta(deadlineDelta)
        {
        }

        ui32 Cookie() const {
            return Cookie0 | (Cookie1 << 15);
        }
    };
public:

    std::optional<ui64> Next(TDuration deadline);
    bool Commit(ui64 offset);
    bool Unlock(ui64 offset);

    bool ProccessDeadlines();

private:
    ui64 DoLock(ui64 offsetDelta, TDuration deadline);
    bool DoCommit(ui64 offsetDelta);
    bool DoUnlock(ui64 offsetDelta);
    bool DoUnlock(TMessage& message, ui64 offset);

    void UpdateBaseDeadline();

private:
    // Первый загруженный оффсет  для обработки. Все сообщения с меньшим оффсетом либо уже закоммичены, либо удалены из партиции.
    // Как часто двигаем FirstOffset? Если FirstUncommittedOffset больше FirstOffset на 1000 (5000? 10000?) либо все сообщения обработаны закончились.
    ui64 FirstOffset;
    // Последний загруженный оффсет для обработки. Всегда <= EndOffset партиции
    ui64 LastOffset;
    // Первый не закоммиченный оффсет для обработки. Всегда <= LastOffset
    ui64 FirstUncommittedOffset;
    // Первый не отданный клиенту для обработки. Всегда <= LastOffset
    ui64 FirstUnlockedOffset;


    // Список сообщений вычитанных ~400KB
    std::deque<TMessage> Messages;

    TDuration BaseDeadline;


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
};



}
