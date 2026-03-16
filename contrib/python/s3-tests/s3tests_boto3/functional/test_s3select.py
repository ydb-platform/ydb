import nose
import random
import string
from nose.plugins.attrib import attr

import uuid
from nose.tools import eq_ as eq

from s3tests_boto3.functional import (
    get_client
    )

region_name = ''

# recurssion function for generating arithmetical expression 
def random_expr(depth):
    # depth is the complexity of expression 
    if depth==1 :
        return str(int(random.random() * 100) + 1)+".0"
    return '(' + random_expr(depth-1) + random.choice(['+','-','*','/']) + random_expr(depth-1) + ')'


def generate_s3select_where_clause(bucket_name,obj_name):

    a=random_expr(4)
    b=random_expr(4)
    s=random.choice([ '<','>','==','<=','>=','!=' ])

    try:
        eval( a )
        eval( b )
    except ZeroDivisionError:
        return

    # generate s3select statement using generated randome expression
    # upon count(0)>0 it means true for the where clause expression
    # the python-engine {eval( conditional expression )} should return same boolean result.
    s3select_stmt =  "select count(0) from stdin where " + a + s + b + ";"

    res = remove_xml_tags_from_result( run_s3select(bucket_name,obj_name,s3select_stmt) ).replace(",","")

    nose.tools.assert_equal(int(res)>0 , eval( a + s + b ))

def generate_s3select_expression_projection(bucket_name,obj_name):

        # generate s3select statement using generated randome expression
        # statement return an arithmetical result for the generated expression.
        # the same expression is evaluated by python-engine, result should be close enough(Epsilon)
        
        e = random_expr( 4 )

        try:
            eval( e )
        except ZeroDivisionError:
            return

        if eval( e ) == 0:
            return

        res = remove_xml_tags_from_result( run_s3select(bucket_name,obj_name,"select " + e + " from stdin;",) ).replace(",","")

        # accuracy level 
        epsilon = float(0.000001) 

        # both results should be close (epsilon)
        assert (1 - (float(res.split("\n")[1]) / eval( e )) ) < epsilon

@attr('s3select')
def get_random_string():

    return uuid.uuid4().hex[:6].upper()

@attr('s3select')
def test_generate_where_clause():

    # create small csv file for testing the random expressions
    single_line_csv = create_random_csv_object(1,1)
    bucket_name = "test"
    obj_name = get_random_string() #"single_line_csv.csv"
    upload_csv_object(bucket_name,obj_name,single_line_csv)
       
    for _ in range(100): 
        generate_s3select_where_clause(bucket_name,obj_name)

@attr('s3select')
def test_generate_projection():

    # create small csv file for testing the random expressions
    single_line_csv = create_random_csv_object(1,1)
    bucket_name = "test"
    obj_name = get_random_string() #"single_line_csv.csv"
    upload_csv_object(bucket_name,obj_name,single_line_csv)
       
    for _ in range(100): 
        generate_s3select_expression_projection(bucket_name,obj_name)
    

def create_csv_object_for_datetime(rows,columns):
        result = ""
        for _ in range(rows):
            row = ""
            for _ in range(columns):
                row = row + "{}{:02d}{:02d}-{:02d}{:02d}{:02d},".format(random.randint(0,100)+1900,random.randint(1,12),random.randint(1,28),random.randint(0,23),random.randint(0,59),random.randint(0,59),)
            result += row + "\n"

        return result

def create_random_csv_object(rows,columns,col_delim=",",record_delim="\n",csv_schema=""):
        result = ""
        if len(csv_schema)>0 :
            result = csv_schema + record_delim

        for _ in range(rows):
            row = ""
            for _ in range(columns):
                row = row + "{}{}".format(random.randint(0,1000),col_delim)
            result += row + record_delim

        return result

def create_random_csv_object_string(rows,columns,col_delim=",",record_delim="\n",csv_schema=""):
        result = ""
        if len(csv_schema)>0 :
            result = csv_schema + record_delim

        for _ in range(rows):
            row = ""
            for _ in range(columns):
                if random.randint(0,9) == 5:
                    row = row + "{}{}".format(''.join(random.choice(string.ascii_letters) for m in range(10)) + "aeiou",col_delim)
                else:
                    row = row + "{}{}".format(''.join("cbcd" + random.choice(string.ascii_letters) for m in range(10)) + "vwxyzzvwxyz" ,col_delim)
                
            result += row + record_delim

        return result


def upload_csv_object(bucket_name,new_key,obj):

        client = get_client()
        client.create_bucket(Bucket=bucket_name)
        client.put_object(Bucket=bucket_name, Key=new_key, Body=obj)

        # validate uploaded object
        c2 = get_client()
        response = c2.get_object(Bucket=bucket_name, Key=new_key)
        eq(response['Body'].read().decode('utf-8'), obj, 's3select error[ downloaded object not equal to uploaded objecy')

    
def run_s3select(bucket,key,query,column_delim=",",row_delim="\n",quot_char='"',esc_char='\\',csv_header_info="NONE"):

    s3 = get_client()

    r = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType='SQL',
        InputSerialization = {"CSV": {"RecordDelimiter" : row_delim, "FieldDelimiter" : column_delim,"QuoteEscapeCharacter": esc_char, "QuoteCharacter": quot_char, "FileHeaderInfo": csv_header_info}, "CompressionType": "NONE"},
        OutputSerialization = {"CSV": {}},
        Expression=query,)
    
    result = ""
    for event in r['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            result += records
    
    return result

def remove_xml_tags_from_result(obj):
    result = ""
    for rec in obj.split("\n"):
        if(rec.find("Payload")>0 or rec.find("Records")>0):
            continue
        result += rec + "\n" # remove by split

    return result

def create_list_of_int(column_pos,obj,field_split=",",row_split="\n"):
    
    list_of_int = [] 
    for rec in obj.split(row_split):
        col_num = 1
        if ( len(rec) == 0):
            continue
        for col in rec.split(field_split):
            if (col_num == column_pos):
                list_of_int.append(int(col))
            col_num+=1

    return list_of_int
       
@attr('s3select')
def test_count_operation():
    csv_obj_name = get_random_string()
    bucket_name = "test"
    num_of_rows = 1234
    obj_to_load = create_random_csv_object(num_of_rows,10)
    upload_csv_object(bucket_name,csv_obj_name,obj_to_load)
    res = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin;") ).replace(",","")

    nose.tools.assert_equal( num_of_rows, int( res ))

@attr('s3select')
def test_column_sum_min_max():
    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = "test"
    upload_csv_object(bucket_name,csv_obj_name,csv_obj)
    
    csv_obj_name_2 = get_random_string()
    bucket_name_2 = "testbuck2"
    upload_csv_object(bucket_name_2,csv_obj_name_2,csv_obj)
    
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select min(int(_1)) from stdin;")  ).replace(",","")
    list_int = create_list_of_int( 1 , csv_obj )
    res_target = min( list_int )

    nose.tools.assert_equal( int(res_s3select), int(res_target))

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select min(int(_4)) from stdin;")  ).replace(",","")
    list_int = create_list_of_int( 4 , csv_obj )
    res_target = min( list_int )

    nose.tools.assert_equal( int(res_s3select), int(res_target))

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select avg(int(_6)) from stdin;")  ).replace(",","")
    list_int = create_list_of_int( 6 , csv_obj )
    res_target = float(sum(list_int ))/10000

    nose.tools.assert_equal( float(res_s3select), float(res_target))
    
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select max(int(_4)) from stdin;")  ).replace(",","")
    list_int = create_list_of_int( 4 , csv_obj )
    res_target = max( list_int )

    nose.tools.assert_equal( int(res_s3select), int(res_target))
    
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select max(int(_7)) from stdin;")  ).replace(",","")
    list_int = create_list_of_int( 7 , csv_obj )
    res_target = max( list_int )

    nose.tools.assert_equal( int(res_s3select), int(res_target))
    
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select sum(int(_4)) from stdin;")  ).replace(",","")
    list_int = create_list_of_int( 4 , csv_obj )
    res_target = sum( list_int )

    nose.tools.assert_equal( int(res_s3select), int(res_target))
    
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select sum(int(_7)) from stdin;")  ).replace(",","")
    list_int = create_list_of_int( 7 , csv_obj )
    res_target = sum( list_int )

    nose.tools.assert_equal(  int(res_s3select) , int(res_target) )

    # the following queries, validates on *random* input an *accurate* relation between condition result,sum operation and count operation.
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name_2,csv_obj_name_2,"select count(0),sum(int(_1)),sum(int(_2)) from stdin where (int(_1)-int(_2)) == 2;" ) )
    count,sum1,sum2,d = res_s3select.split(",")

    nose.tools.assert_equal( int(count)*2 , int(sum1)-int(sum2 ) )

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0),sum(int(_1)),sum(int(_2)) from stdin where (int(_1)-int(_2)) == 4;" ) ) 
    count,sum1,sum2,d = res_s3select.split(",")

    nose.tools.assert_equal( int(count)*4 , int(sum1)-int(sum2) )

@attr('s3select')
def test_nullif_expressions():

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = "test"
    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin where nullif(_1,_2) is null ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin where _1 == _2  ;")  ).replace("\n","")

    nose.tools.assert_equal( res_s3select_nullif, res_s3select)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin where not nullif(_1,_2) is null ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin where _1 != _2  ;")  ).replace("\n","")

    nose.tools.assert_equal( res_s3select_nullif, res_s3select)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin where  nullif(_1,_2) == _1 ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin where _1 != _2  ;")  ).replace("\n","")

    nose.tools.assert_equal( res_s3select_nullif, res_s3select)

@attr('s3select')
def test_lowerupper_expressions():

    csv_obj = create_random_csv_object(1,10)

    csv_obj_name = get_random_string()
    bucket_name = "test"
    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select lower("AB12cd$$") from stdin ;')  ).replace("\n","")

    nose.tools.assert_equal( res_s3select, "ab12cd$$,")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select upper("ab12CD$$") from stdin ;')  ).replace("\n","")

    nose.tools.assert_equal( res_s3select, "AB12CD$$,")

@attr('s3select')
def test_in_expressions():

    # purpose of test: engine is process correctly several projections containing aggregation-functions 
    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = "test"
    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from stdin where int(_1) in(1);')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from stdin where int(_1) == 1;')).replace("\n","")

    nose.tools.assert_equal( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from stdin where int(_1) in(1,0);')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from stdin where int(_1) == 1 or int(_1) == 0;')).replace("\n","")

    nose.tools.assert_equal( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_2) from stdin where int(_2) in(1,0,2);')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_2) from stdin where int(_2) == 1 or int(_2) == 0 or int(_2) == 2;')).replace("\n","")

    nose.tools.assert_equal( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_2) from stdin where int(_2)*2 in(int(_3)*2,int(_4)*3,int(_5)*5);')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_2) from stdin where int(_2)*2 == int(_3)*2 or int(_2)*2 == int(_4)*3 or int(_2)*2 == int(_5)*5;')).replace("\n","")

    nose.tools.assert_equal( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from stdin where character_length(_1) == 2 and substring(_1,2,1) in ("3");')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from stdin where _1 like "_3";')).replace("\n","")

    nose.tools.assert_equal( res_s3select_in, res_s3select )

@attr('s3select')
def test_like_expressions():

    csv_obj = create_random_csv_object_string(1000,10)

    csv_obj_name = get_random_string()
    bucket_name = "test"
    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from stdin where _1 like "%aeio%";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from stdin where substring(_1,11,4) == "aeio" ;')).replace("\n","")

    nose.tools.assert_equal( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from stdin where _1 like "cbcd%";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from stdin where substring(_1,1,4) == "cbcd";')).replace("\n","")

    nose.tools.assert_equal( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from stdin where _3 like "%y[y-z]";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from stdin where substring(_3,char_length(_3),1) between "y" and "z" and substring(_3,char_length(_3)-1,1) == "y";')).replace("\n","")

    nose.tools.assert_equal( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from stdin where _2 like "%yz";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from stdin where substring(_2,char_length(_2),1) == "z" and substring(_2,char_length(_2)-1,1) == "y";')).replace("\n","")

    nose.tools.assert_equal( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from stdin where _3 like "c%z";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from stdin where substring(_3,char_length(_3),1) == "z" and substring(_3,1,1) == "c";')).replace("\n","")

    nose.tools.assert_equal( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from stdin where _2 like "%xy_";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from stdin where substring(_2,char_length(_2)-1,1) == "y" and substring(_2,char_length(_2)-2,1) == "x";')).replace("\n","")

    nose.tools.assert_equal( res_s3select_in, res_s3select )


@attr('s3select')
def test_complex_expressions():

    # purpose of test: engine is process correctly several projections containing aggregation-functions 
    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = "test"
    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select min(int(_1)),max(int(_2)),min(int(_3))+1 from stdin;")).replace("\n","")

    min_1 = min ( create_list_of_int( 1 , csv_obj ) )
    max_2 = max ( create_list_of_int( 2 , csv_obj ) )
    min_3 = min ( create_list_of_int( 3 , csv_obj ) ) + 1

    __res = "{},{},{},".format(min_1,max_2,min_3)
    
    # assert is according to radom-csv function 
    nose.tools.assert_equal( res_s3select, __res )

    # purpose of test that all where conditions create the same group of values, thus same result
    res_s3select_substring = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select min(int(_2)),max(int(_2)) from stdin where substring(_2,1,1) == "1"')).replace("\n","")

    res_s3select_between_numbers = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select min(int(_2)),max(int(_2)) from stdin where int(_2)>=100 and int(_2)<200')).replace("\n","")

    res_s3select_eq_modolu = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select min(int(_2)),max(int(_2)) from stdin where int(_2)/100 == 1 or int(_2)/10 == 1 or int(_2) == 1')).replace("\n","")

    nose.tools.assert_equal( res_s3select_substring, res_s3select_between_numbers)

    nose.tools.assert_equal( res_s3select_between_numbers, res_s3select_eq_modolu)
    
@attr('s3select')
def test_alias():

    # purpose: test is comparing result of exactly the same queries , one with alias the other without.
    # this test is setting alias on 3 projections, the third projection is using other projection alias, also the where clause is using aliases
    # the test validate that where-clause and projections are executing aliases correctly, bare in mind that each alias has its own cache,
    # and that cache need to be invalidate per new row. 

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = "test"
    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select int(_1) as a1, int(_2) as a2 , (a1+a2) as a3 from stdin where a3>100 and a3<300;")  ).replace(",","")

    res_s3select_no_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select int(_1),int(_2),int(_1)+int(_2) from stdin where (int(_1)+int(_2))>100 and (int(_1)+int(_2))<300;")  ).replace(",","")

    nose.tools.assert_equal( res_s3select_alias, res_s3select_no_alias)


@attr('s3select')
def test_alias_cyclic_refernce():

    number_of_rows = 10000
    
    # purpose of test is to validate the s3select-engine is able to detect a cyclic reference to alias.
    csv_obj = create_random_csv_object(number_of_rows,10)

    csv_obj_name = get_random_string()
    bucket_name = "test"
    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select int(_1) as a1,int(_2) as a2, a1+a4 as a3, a5+a1 as a4, int(_3)+a3 as a5 from stdin;")  )

    find_res = res_s3select_alias.find("number of calls exceed maximum size, probably a cyclic reference to alias")
    
    assert int(find_res) >= 0 

@attr('s3select')
def test_datetime():

    # purpose of test is to validate date-time functionality is correct,
    # by creating same groups with different functions (nested-calls) ,which later produce the same result 

    csv_obj = create_csv_object_for_datetime(10000,1)

    csv_obj_name = get_random_string()
    bucket_name = "test"

    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_date_time = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  stdin where extract("year",timestamp(_1)) > 1950 and extract("year",timestamp(_1)) < 1960;')  )

    res_s3select_substring = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  stdin where int(substring(_1,1,4))>1950 and int(substring(_1,1,4))<1960;')  )

    nose.tools.assert_equal( res_s3select_date_time, res_s3select_substring)

    res_s3select_date_time = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  stdin where  datediff("month",timestamp(_1),dateadd("month",2,timestamp(_1)) ) == 2;')  )

    res_s3select_count = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  stdin;')  )

    nose.tools.assert_equal( res_s3select_date_time, res_s3select_count)

    res_s3select_date_time = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  stdin where datediff("year",timestamp(_1),dateadd("day", 366 ,timestamp(_1))) == 1 ;')  )

    nose.tools.assert_equal( res_s3select_date_time, res_s3select_count)

    # validate that utcnow is integrate correctly with other date-time functions 
    res_s3select_date_time_utcnow = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  stdin where datediff("hours",utcnow(),dateadd("day",1,utcnow())) == 24 ;')  )

    nose.tools.assert_equal( res_s3select_date_time_utcnow, res_s3select_count)

@attr('s3select')
def test_csv_parser():

    # purpuse: test default csv values(, \n " \ ), return value may contain meta-char 
    # NOTE: should note that default meta-char for s3select are also for python, thus for one example double \ is mandatory

    csv_obj = ',first,,,second,third="c31,c32,c33",forth="1,2,3,4",fifth="my_string=\\"any_value\\" , my_other_string=\\"aaaa,bbb\\" ",' + "\n"
    csv_obj_name = get_random_string()
    bucket_name = "test"

    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    # return value contain comma{,}
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _6 from stdin;")  ).replace("\n","")
    nose.tools.assert_equal( res_s3select_alias, 'third="c31,c32,c33",')

    # return value contain comma{,}
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _7 from stdin;")  ).replace("\n","")
    nose.tools.assert_equal( res_s3select_alias, 'forth="1,2,3,4",')

    # return value contain comma{,}{"}, escape-rule{\} by-pass quote{"} , the escape{\} is removed.
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _8 from stdin;")  ).replace("\n","")
    nose.tools.assert_equal( res_s3select_alias, 'fifth="my_string="any_value" , my_other_string="aaaa,bbb" ",')

    # return NULL as first token
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _1 from stdin;")  ).replace("\n","")
    nose.tools.assert_equal( res_s3select_alias, ',')

    # return NULL in the middle of line
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _3 from stdin;")  ).replace("\n","")
    nose.tools.assert_equal( res_s3select_alias, ',')

    # return NULL in the middle of line (successive)
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _4 from stdin;")  ).replace("\n","")
    nose.tools.assert_equal( res_s3select_alias, ',')

    # return NULL at the end line
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _9 from stdin;")  ).replace("\n","")
    nose.tools.assert_equal( res_s3select_alias, ',')

@attr('s3select')
def test_csv_definition():

    number_of_rows = 10000

    #create object with pipe-sign as field separator and tab as row delimiter.
    csv_obj = create_random_csv_object(number_of_rows,10,"|","\t")

    csv_obj_name = get_random_string()
    bucket_name = "test"

    upload_csv_object(bucket_name,csv_obj_name,csv_obj)
   
    # purpose of tests is to parse correctly input with different csv defintions  
    res = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin;","|","\t") ).replace(",","")

    nose.tools.assert_equal( number_of_rows, int(res))
    
    # assert is according to radom-csv function 
    # purpose of test is validate that tokens are processed correctly
    res_s3select = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select min(int(_1)),max(int(_2)),min(int(_3))+1 from stdin;","|","\t") ).replace("\n","")

    min_1 = min ( create_list_of_int( 1 , csv_obj , "|","\t") )
    max_2 = max ( create_list_of_int( 2 , csv_obj , "|","\t") )
    min_3 = min ( create_list_of_int( 3 , csv_obj , "|","\t") ) + 1

    __res = "{},{},{},".format(min_1,max_2,min_3)
    nose.tools.assert_equal( res_s3select, __res )


@attr('s3select')
def test_schema_definition():

    number_of_rows = 10000

    # purpose of test is to validate functionality using csv header info
    csv_obj = create_random_csv_object(number_of_rows,10,csv_schema="c1,c2,c3,c4,c5,c6,c7,c8,c9,c10")

    csv_obj_name = get_random_string()
    bucket_name = "test"

    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    # ignoring the schema on first line and retrieve using generic column number
    res_ignore = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select _1,_3 from stdin;",csv_header_info="IGNORE") ).replace("\n","")

    # using the scheme on first line, query is using the attach schema
    res_use = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select c1,c3 from stdin;",csv_header_info="USE") ).replace("\n","")
    
    # result of both queries should be the same
    nose.tools.assert_equal( res_ignore, res_use)

    # using column-name not exist in schema
    res_multiple_defintion = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select c1,c10,int(c11) from stdin;",csv_header_info="USE") ).replace("\n","")

    assert res_multiple_defintion.find("alias {c11} or column not exist in schema") > 0

    # alias-name is identical to column-name
    res_multiple_defintion = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select int(c1)+int(c2) as c4,c4 from stdin;",csv_header_info="USE") ).replace("\n","")

    assert res_multiple_defintion.find("multiple definition of column {c4} as schema-column and alias") > 0

@attr('s3select')
def test_when_than_else_expressions():

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = "test"
    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select case when cast(_1 as int)>100 and cast(_1 as int)<200 than "(100-200)" when cast(_1 as int)>200 and cast(_1 as int)<300 than "(200-300)" else "NONE" end from s3object;')  ).replace("\n","")

    count1 = res_s3select.count("(100-200)")  

    count2 = res_s3select.count("(200-300)") 

    count3 = res_s3select.count("NONE")

    res = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where  cast(_1 as int)>100 and cast(_1 as int)<200  ;')  ).replace("\n","")

    res1 = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where  cast(_1 as int)>200 and cast(_1 as int)<300  ;')  ).replace("\n","")
    
    res2 = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where  cast(_1 as int)<=100 or cast(_1 as int)>=300 or cast(_1 as int)==200  ;')  ).replace("\n","")

    nose.tools.assert_equal( str(count1) + ',', res)

    nose.tools.assert_equal( str(count2) + ',', res1)

    nose.tools.assert_equal( str(count3) + ',', res2)

@attr('s3select')
def test_coalesce_expressions():

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = "test"
    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where char_length(_3)>2 and char_length(_4)>2 and cast(substring(_3,1,2) as int) == cast(substring(_4,1,2) as int);')  ).replace("\n","")  

    res_null = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where cast(_3 as int)>99 and cast(_4 as int)>99 and coalesce(nullif(cast(substring(_3,1,2) as int),cast(substring(_4,1,2) as int)),7) == 7;' ) ).replace("\n","") 

    nose.tools.assert_equal( res_s3select, res_null)

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select coalesce(nullif(_5,_5),nullif(_1,_1),_2) from stdin;')  ).replace("\n","") 

    res_coalesce = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select coalesce(_2) from stdin;')  ).replace("\n","")   

    nose.tools.assert_equal( res_s3select, res_coalesce)


@attr('s3select')
def test_cast_expressions():

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = "test"
    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where cast(_3 as int)>999;')  ).replace("\n","")  

    res = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where char_length(_3)>3;')  ).replace("\n","") 

    nose.tools.assert_equal( res_s3select, res)

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where cast(_3 as int)>99 and cast(_3 as int)<1000;')  ).replace("\n","")  

    res = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where char_length(_3)==3;')  ).replace("\n","") 

    nose.tools.assert_equal( res_s3select, res)

@attr('s3select')
def test_version():

    return
    number_of_rows = 1

    # purpose of test is to validate functionality using csv header info
    csv_obj = create_random_csv_object(number_of_rows,10)

    csv_obj_name = get_random_string()
    bucket_name = "test"

    upload_csv_object(bucket_name,csv_obj_name,csv_obj)

    res_version = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select version() from stdin;") ).replace("\n","")

    nose.tools.assert_equal( res_version, "41.a," )

