you should add the local jar in --jars

in order to use scala UDFs in python you need to wrap it in the following template


from py4j.java_gateway import java_import
from pyspark.sql.column import Column, _to_java_column, _to_seq
jvm = sc._gateway.jvm
java_import(jvm, "com.gett.s2udfs")

def udf_name(a,b):
     method = sc._jvm.com.gett.s2udfs.S2Udfs.udf_name().apply
     return Column(_groupConcat(_to_seq(sc, [a,b], _to_java_column)))
 