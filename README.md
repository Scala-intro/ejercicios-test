# ejercicios-test

0. [Revisar SDK del Proyecto](#schema0)
1. [Scala + Spark: Transformaciones y Acciones](#schema1)

<hr>

<a name="schema0"></a>

# 0. Revisa el SDK del proyecto
- Ve a File → Project Structure → SDKs.
- Asegúrate de que estás usando la versión correcta de Java (JDK 8 o JDK 11 es recomendable para Spark).
`/Library/Java/JavaVirtualMachines/openjdk-11.jdk/Contents/Home`, en mi caso.



<hr>

<a name="schema1"></a>

# 1.Scala + Spark: Transformaciones y Acciones
Dado un dataset de transacciones bancarias, realiza algunas operaciones con Spark en Scala.

1. Cuenta cuántos depósitos y retiros hay por cliente.
2. Calcula el total de depósitos por cliente.
3. Filtra las transacciones mayores a 500.

[Ejercicios](/src/main/scala/test.scala)