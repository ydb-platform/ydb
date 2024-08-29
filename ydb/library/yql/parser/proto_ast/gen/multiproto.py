import os
import sys

NSPLIT=10

def main(argv):
    input_dir="."
    output_dir="."
    name=sys.argv[1]
    if len(argv)>2:
        input_dir=argv[2]
    if len(argv)>3:
        output_dir=argv[3]
    print("name:",name)
    print("input_dir:",input_dir)
    print("output_dir:",output_dir)
    
    in_h=os.path.join(input_dir,name + ".pb.h")
    in_cpp=os.path.join(input_dir,name + ".pb.cc")
    out_h=os.path.join(output_dir,name + ".pb.main.h")
    out_cpp_template=os.path.join(output_dir,name + ".pb.I")

    with open(out_h,"w") as out_file:
       with open(in_h,"r") as in_file:
           for line in in_file:
              line = line.replace("inline void RegisterArenaDtor","void RegisterArenaDtor")
              out_file.write(line) 

    for i in range(0,2 + NSPLIT):
        with open(out_cpp_template.replace("I","code" + str(i) + ".cc" if i<NSPLIT else "data.cc" if i==NSPLIT else "classes.h"),"w") as out_file:
            with open(in_cpp,"r") as in_file:
                line = line.replace("inline ","")
                statement_index=0
                current_types=set()
                is_data_stmt=False
                extern_data=False
                extern_code=False
                in_class_def=False
                for line in in_file:
                    if line.startswith("#include") and name + ".pb.h" in line:
                        out_file.write('#include "' + name + '.pb.main.h"\n')
                        if i!=NSPLIT+1:
                            out_file.write('#include "' + name + '.pb.classes.h"\n')
                        continue
                    if line.strip()=="PROTOBUF_PRAGMA_INIT_SEG":
                        out_file.write(line)
                        break
                    out_file.write(line)
                for line in in_file:
                    line=line.replace("inline ","")
                    if 'Generated::' in line and line.endswith('_default_instance_._instance,\n'):
                        line = f'reinterpret_cast<const ::_pb::Message*>({line.removesuffix('._instance,\n')}),'
                    if line.startswith("#"):
                        out_file.write(line)
                        continue
                    if line.startswith("namespace") or line.startswith("PROTOBUF_NAMESPACE_OPEN"):
                        open_namespace = True
                        out_file.write(line)
                        continue
                    if (line.startswith("}  // namespace") or line.startswith("PROTOBUF_NAMESPACE_CLOSE")) and open_namespace:
                        open_namespace = False
                        out_file.write(line)
                        continue
                    if in_class_def:
                        if (i==NSPLIT+1):
                           out_file.write(line)
                        if line.startswith("};"):
                           in_class_def=False
                        continue
                    if line.startswith("PROTOBUF_ATTRIBUTE_NO_DESTROY PROTOBUF_CONSTINIT"):
                        # MOD1 MOD2 MOD3 ... type_name varibale_name;
                        type_name=line.split(" ")[-2]
                        if type_name in current_types:
                            out_file.write(line)
                        continue
                    if line.startswith("static ") or (line.startswith("const ") and ("[]" in line or "=" in line)) or line.startswith("PROTOBUF_ATTRIBUTE_WEAK") or line.startswith("PROTOBUF_ATTRIBUTE_INIT_PRIORITY2"):
                        is_data_stmt = True
                        extern_data = "file_level_metadata" in line or ("descriptor_table" in line and "once" in line)
                        extern_code = line.startswith("PROTOBUF_ATTRIBUTE_WEAK")
                    if line.startswith("class"):
                       in_class_def=True
                       if i==NSPLIT+1:
                          out_file.write(line)
                       continue
                    if not is_data_stmt and (statement_index % NSPLIT)==i:
                        if line.startswith("struct"):
                            current_types.add(line.split(" ")[1])
                        out_file.write(line)
                    if is_data_stmt and i==NSPLIT:
                        if extern_data:
                           line = line.replace("static ","")
                        out_file.write(line)
                    if is_data_stmt and i<NSPLIT:
                        if extern_data or extern_code:
                            if extern_data:
                                line = "extern " + line.replace("static ","").replace(" = {",";")
                            if extern_code:
                                if not "PROTOBUF_ATTRIBUTE_WEAK" in line:
                                    continue
                                line = "extern " + line.replace(" {",";")
                            out_file.write(line)
                            extern_data = False
                            extern_code = False
                    if line.startswith("}"):
                        if is_data_stmt:
                            is_data_stmt=False
                            extern_data = False
                            extern_code = False
                        else:
                            statement_index += 1

if __name__ == "__main__":
    main(sys.argv)
