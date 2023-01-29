#!/usr/bin/env python3

NON_REPEATED = \
"""
    /// {field_type} {field_name}
    {field_type} {field_name}{field_default};
    bool has{field_name};
    
    void Set{field_name}(const {field_type}& value) {{
        {field_name} = value;
        has{field_name} = true;
    }}
    
    bool Has{field_name}() const {{
        return has{field_name};
    }}
    
    const {field_type}& Get{field_name}() const {{
        return {field_name};
    }}
    
    void Clear{field_name}() {{
        {field_name} = {{}};
        has{field_name} = false;
    }}
    
    {field_type}* Mutable{field_name}() {{
        return &{field_name};
    }}

"""

REPEATED = \
"""
    /// {field_type} {field_name} <repeated>
    std::vector<{field_type}> {field_name};
    
    const std::vector<{field_type}>& Get{field_name}() const {{
        return {field_name};
    }}
    
    {field_type}* Add{field_name}() {{
        {field_name}.push_back({{}});
        return &{field_name}.back();
    }}
    
    size_t {field_name}Size() {{
        return {field_name}.size();
    }}

"""

# TODO: add `ShortDebugString`, `CopyFrom`,   `SerializeToZeroCopyStream`, `ParseFromZeroCopyStream`, `ParseFromString`
STRUCT_WIDE = \
"""
    /// struct-wide methods
    TString GetTypeName() const {{
        return "{type_name}";
    }}
    
    int ByteSize() const {{
        return {sizeof};
    }}
    
    void CopyFrom(const {type_name}& other) {{
        *this = other;
    }}
    
    bool serializeHelper(NProtoBuf::io::ZeroCopyOutputStream *output, void* data, int size) {{
        return output->Next(&data, &size);
    }}
    
    bool SerializeToZeroCopyStream(NProtoBuf::io::ZeroCopyOutputStream *output) const {{
        {serialize_code}
    }}
    
}};\n\n"""
