# ejercicios-test

0. [Revisar SDK del Proyecto](#schema0)
1. [Scala + Spark: Transformaciones y Acciones](#schema1)
2. [Modelado de Datos: Esquema de Ventas en Spark](#schema2)




<hr>

<a name="schema0"></a>

# 0. Revisa el SDK del proyecto
- Ve a File → Project Structure → SDKs.
- Asegúrate de que estás usando la versión correcta de Java (JDK 8 o JDK 11 es recomendable para Spark).
`/Library/Java/JavaVirtualMachines/openjdk-11.jdk/Contents/Home`, en mi caso.



<hr>

<a name="schema1"></a>

# 1. Scala + Spark: Transformaciones y Acciones
Dado un dataset de transacciones bancarias, realiza algunas operaciones con Spark en Scala.

1. Cuenta cuántos depósitos y retiros hay por cliente.
2. Calcula el total de depósitos por cliente.
3. Filtra las transacciones mayores a 500.

[Ejercicios](/src/main/scala/test.scala)


<hr>

<a name="schema2"></a>


# 2. Modelado de Datos: Esquema de Ventas en Spark
Diseña un modelo de datos en Scala + Spark para una tienda en línea que maneja productos, clientes y órdenes.


1. Crea DataFrames para cada entidad.
2. Realiza una unión entre Órdenes y Clientes para ver qué clientes compraron qué productos.
3. Calcula el total gastado por cada cliente.