--sanitizer ignore memory
$script = @@ 
from yql.typing import *

def primitive(a0:Bool,a1:Int8,a2:Uint8,a3:Int16,a4:Uint16,a5:Int32,a6:Uint32,
   a7:Int64,a8:Uint64,a9:Float,a10:Double,a11:String,a12:Utf8,a13:Yson,a14:Json,
   a15:Uuid,a16:Date,a17:Datetime,a18:Timestamp,a19:Interval,a20:TzDate,
   a21:TzDatetime,a22:TzTimestamp)->Decimal(10,3):
   pass

def singletons(a0:Void,a1:Null,a2:EmptyStruct,a3:EmptyTuple)->Void:
   pass

def containers(a0:Optional[Int32],a1:List[List[Bool]],a2:Stream[String],a3:Dict[Int32,String],
   a4:Tuple[Int32,String],a5:Tuple[Int32],a6:Struct["a":Int32,"b":String],a7:Struct["a":Int32],
   a8:Variant[Int32,String],a9:Variant[Int32],a10:Variant["a":Int32,"b":String],a11:Variant["a":Int32])->List[String]:
   pass

def special(a0:Resource["Python3"],a1:Tagged[Int32,"foo"])->Void:
   pass

def c0()->Callable[0,Int32]: pass
def c1()->Callable[1,Int32,Optional[List[Int32]]]: pass
def c2()->Callable[1,Int32,Int32,Optional[List[Int32]]]: pass
def c3()->Callable[0,Int32,"a":Int32:{AutoMap}]: pass
def c4()->Callable[0,Int32,"":Int32:{AutoMap}]: pass
def c5()->Callable[0,Int32,"":Int32:{}]: pass
def c6()->Callable[0,Int32,"foo":Int32]: pass

def f0(x:Optional[Int32]=None,y:Optional[Int32]=None)->Void: pass
def f1(x:Optional[Int32],y:Optional[Int32]=None)->Void: pass
def f2(x:Optional[Int32],y:Optional[Int32])->Void: pass
def f3(x:slice("",Int32,{AutoMap}), y:slice("name",String))->Void: pass

@@;

$t = ($name)->{
    return FormatType(EvaluateType(
       ParseTypeHandle(Core::PythonFuncSignature(AsAtom("Python3"), $script, $name))));
};

-- Singletons

select $t("primitive");
select $t("singletons");

-- Containers & Special

select $t("containers");
select $t("special");

-- Callable   
select 
  $t("c0") as c0,
  $t("c1") as c1,
  $t("c2") as c2,
  $t("c3") as c3,
  $t("c4") as c4,
  $t("c5") as c5,
  $t("c6") as c6;  

-- Top level
select
  $t("f0") as f0,
  $t("f1") as f1,
  $t("f2") as f2,
  $t("f3") as f3;
