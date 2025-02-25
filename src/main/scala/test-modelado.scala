import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object testModelado extends App {

    val spark = SparkSession.builder
      .appName("TestModelado")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Definición de case classes
    case class Producto(id: Int, nombre: String, precio: Double)
    case class Cliente(id: Int, nombre: String, email: String)
    case class Orden(id: Int, clienteId: Int, productoId: Int, cantidad: Int, fecha: String)


    // Datos de ejemplo
    val productos = Seq(
      Producto(1, "Laptop", 1200.0),
      Producto(2, "Mouse", 25.0),
      Producto(3, "Teclado", 45.0)
    )

    val clientes = Seq(
      Cliente(1, "Ana", "ana@email.com"),
      Cliente(2, "Juan", "juan@email.com"),
      Cliente(3, "Sofía", "sofia@email.com")
    )

    val ordenes = Seq(
      Orden(1, 1, 1, 1, "2025-02-01"), // Ana compra 1 Laptop
      Orden(2, 1, 2, 2, "2025-02-02"), // Ana compra 2 Mouse
      Orden(3, 2, 3, 1, "2025-02-03"), // Juan compra 1 Teclado
      Orden(4, 3, 1, 1, "2025-02-04")  // Sofía compra 1 Laptop
    )

    // Crea DataFrames para cada entidad.


    val dfProductos = productos.toDF()
    val dfClientes = clientes.toDF()
    val dfOrdenes = ordenes.toDF()

    dfProductos.show()
    dfClientes.show()
    dfOrdenes.show()
    // Realiza una unión entre Órdenes y Clientes para ver qué clientes compraron qué productos.

  val ordenesClientes = dfOrdenes
    .join(dfClientes, dfOrdenes("clienteId") === dfClientes("id"))
    .join(dfProductos, dfOrdenes("productoId") === dfProductos("id"))
    .select(dfOrdenes("id").alias("OrdenID"),
      dfClientes("nombre").alias("ClienteNombre"),
      dfProductos("nombre").alias("ProductoNombre"),
      dfOrdenes("cantidad"),
      dfOrdenes("fecha"))

  println("Clientes y sus compras:")
  ordenesClientes.show()

    // Calcula el total gastado por cada cliente.


  // Cálculo del total gastado por cada cliente
  val totalGastado = dfOrdenes
    .join(dfProductos, dfOrdenes("productoId") === dfProductos("id"))
    .join(dfClientes, dfOrdenes("clienteId") === dfClientes("id"))
    .groupBy(dfClientes("nombre"))
    .agg(sum($"cantidad" * $"precio").alias("Total_Gastado"))

  println("Total gastado por cliente:")
  totalGastado.show()



  spark.close()
}