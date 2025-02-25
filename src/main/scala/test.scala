import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ejercicioTest{
  def main (agrs: Array[String]) : Unit = {
    val spark = SparkSession.builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      ("2025-02-01", "Cliente1", 1000.0, "Depósito"),
      ("2025-02-02", "Cliente2", 500.0, "Retiro"),
      ("2025-02-03", "Cliente1", 700.0, "Depósito"),
      ("2025-02-04", "Cliente3", 200.0, "Retiro"),
      ("2025-02-05", "Cliente2", 400.0, "Depósito")
    )

    val df = data.toDF("Fecha", "Cliente", "Monto", "Tipo")


    // 1. Cuenta cuántos depósitos y retiros hay por cliente.
    val movimientos = df.groupBy("Cliente","Tipo").agg(count("*").alias("Cantidad"))
    movimientos.show()
    // 2. Calcula el total de depósitos por cliente.

    val depositos = df.filter($"Tipo" === "Depósito")
      .groupBy("Cliente")
      .agg(sum("Monto").alias("TotalDeposito"))

    depositos.show()
    // 3. Filtra las transacciones mayores a 500.

    val transaccionmayor500 = df.filter($"Monto" > 500)
    transaccionmayor500.show()





  }
}

