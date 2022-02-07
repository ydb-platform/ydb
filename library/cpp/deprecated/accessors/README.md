Unified accessors for Arcadia containers and user types.

Accessors implemented here mix different kinds of access at the wrong abstraction level, so they shouldn't be used.

If you want begin/end/size for your containers, use std::begin, std::end, std::size. If you need generic reserve / resize / clear / insert, just use appropriate container methods or do your own overloads in place.
