#pragma once 
 
class TAtomicRefCountedObject: public TAtomicRefCount<TAtomicRefCountedObject> {
    virtual ~TAtomicRefCountedObject() {
    }
}; 
