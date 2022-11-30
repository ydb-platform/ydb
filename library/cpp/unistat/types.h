#pragma once

#include <util/generic/string.h>

enum class EAggregationType {
    Average /* "aver"   Брать среднее среди всех полученных хостовых значений             */,
    HostHistogram /* "hgram"  Выполнять слияние всех полученных хостовых значений в гистограмму */,
    Max /* "max"    Брать максимальное среди всех полученных хостовых значений        */,
    Min /* "min"    Брать минимальное среди всех полученных хостовых значений         */,
    Sum /* "summ"   Брать сумму всех полученных хостовых значений                     */,
    SumOne /* "sumone" summ c None для отсутсвия сигналов                                */,
    LastValue /* "trnsp"  Брать последнее среди всех полученных хостовых значений           */
};

const TString& ToString(EAggregationType x);
