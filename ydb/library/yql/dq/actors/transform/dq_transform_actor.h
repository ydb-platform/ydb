#pragma once

namespace NYql::NDq {

class IDqTransformActor {
public:
    virtual void DoTransform() = 0;
    virtual ~IDqTransformActor() = default;
};

}