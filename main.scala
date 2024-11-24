package Practica

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object Main {
  def main(args: Array[String]): Unit = {

    // Inicializar Spark
    val spark: SparkSession = SparkSession.builder().appName("SparkTest").master("local[*]").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").config("spark.blockManager.port", "0").config("spark.memory.fraction", "0.8").config("spark.storage.memoryFraction", "0.5").config("spark.driver.host", "127.0.0.1").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    // Llamar al Ejercicio 1: Operaciones básicas con DataFrame
    println("== Ejercicio 1 ==")
    examen.ejercicio1(examen.df)(spark)

    // Llamar al Ejercicio 2: UDF (User Defined Function) para determinar par/impar
    println("== Ejercicio 2 ==")
    val numeros = Seq(1, 2, 3, 4, 5, 6).toDF("numero")
    val numerosConParidad = examen.ejercicio2(numeros)(spark)
    numerosConParidad.show()

    // Llamar al Ejercicio 3: Joins y agregaciones
    println("== Ejercicio 3 ==")
    val estudiantes = Seq((1, "Pablo"), (2, "Lucía"), (3, "Carlos")).toDF("id", "nombre")
    val calificaciones = Seq(
      (1, "Matemáticas", 9.0),
      (1, "Historia", 8.5),
      (2, "Matemáticas", 7.5),
      (3, "Historia", 6.8)
    ).toDF("id_estudiante", "asignatura", "calificacion")
    val promedioCalificaciones = examen.ejercicio3(estudiantes, calificaciones)
    promedioCalificaciones.show()

    // Llamar al Ejercicio 4: Uso de RDDs para contar palabras
    println("== Ejercicio 4 ==")
    val palabras = List("manzana", "banana", "pera", "manzana", "banana", "manzana")
    val conteoPalabras: RDD[(String, Int)] = examen.ejercicio4(palabras)(spark)
    conteoPalabras.collect().foreach { case (palabra, conteo) =>
      println(s"$palabra: $conteo")
    }
    // Llamar al Ejercicio 5: Procesamiento de archivos
    println("== Ejercicio 5 ==")
    val ventasDF: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true") 
      .csv("src/main/scala/Practica/ventas.csv")

    val ingresosPorProducto = examen.ejercicio5(ventasDF)(spark)
    println("Ingresos totales por producto:")
    ingresosPorProducto.show()
  }
}
