
import org.apache.spark.SparkConf
import java.util.Random
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import java.io.PrintWriter
import java.io.FileWriter
import breeze.linalg.{Vector, DenseVector}
import scala.io.Source
import scopt.OptionParser

// model is 1/n||X w - y||^2 + lambda ||w||_1  
object doStreamLasso {
	  var numDataInArray = 0
	  var regPara = 0.1
	  var epsilon = 0.01
	  var numDataInBatch = 0
	  var dimenOfData = 9
	  var w_stream = DenseVector.zeros[Double](dimenOfData+1)
	  var dataPointArray = List[DataPoint]()
	  case class Params(
	  dimenOfData: Int = 9,
	  testFileName: String = null,
	  outPutFileName: String = null,
	  IP: String = "localhost",
	  port: Int = 9999,
	  step: Double = 0.1,
	  regPara: Double = 0.1,
	  epsilon:Double = 0.01,
	  numDataInBatch: Int = 50,
	  numIteration: Int = 50,
	  timeIntertoGetData: Int = 3)
	  case class DataPoint(x: Vector[Double], y: Int)
    def main(args: Array[String]) {
	    val defaultParams = Params()
		val parser = new OptionParser[Params]("doStreamBatchLogisticRegression") {
	    head("An example app for running Streaming Logistic Regression.")
	    
        opt[String]("IP")
        .text(s"IP to get streaming data, default:" + defaultParams.IP)
        .action((x, c) => c.copy(IP = x))
        opt[String]("testFileName")
        .text("File name of file contains test data")
        .action((x, c) => c.copy(testFileName = x))
        opt[Int]("port")
        .text(s"The port to get steraming data, default: ${defaultParams.port}")
        .action((x, c) => c.copy(port = x))
        opt[Double]("step")
        .text(s"The step size for gradient descent in logistic regression, default: ${defaultParams.step}")
        .action((x, c) => c.copy(step = x))
        opt[Double]("regPara")
        .text(s"The parameter of regression, default: ${defaultParams.regPara}")
        .action((x, c) => c.copy(regPara = x))
        opt[Double]("epsilon")
        .text(s"The parameter of Huber function, default: ${defaultParams.epsilon}")
        .action((x, c) => c.copy(epsilon = x))
        opt[Int]("numDataInBatch")
        .text("number of data in a batch, default: ${defaultParams.numDataInBatch}")
        .action((x, c) => c.copy(numDataInBatch = x))
        opt[Int]("numIteration")
        .text("number of iterations in each logistic regerssion, default: ${defaultParams.numIteration}")
        .action((x, c) => c.copy(numIteration = x))
        
        opt[Int]("dimenOfData")
        .required()
        .text("dimensions of features, must be assigned")
        .action((x,c) => c.copy(dimenOfData = x))
        arg[String]("<outPutFileName>")
        .required()
        .text("The file to write the output parameters ")
        .action((x, c) => c.copy(outPutFileName = x))
        note(
        """
          |For example, the following command runs this app and get streaming data from a local host IP:
          |
          | bin/spark-submit --class doStreamBatchLogisticRegression \
          |  --master local[2] \
          |  --jar StreamBatchLogisticRegression.jar \
          |  doStreamBatchLogisticRegression.jar \
          |  --step 0.1 --numDataInBatch 40 --dimenOfData 9 --regPara 0.1 \
          |  parameters.txt
        """.stripMargin)
	    }
	  println("Prepared to run")
	 parser.parse(args, defaultParams).map { params =>
	 dimenOfData = params.dimenOfData
	 numDataInBatch = params.numDataInBatch
	 w_stream = DenseVector.zeros[Double](dimenOfData+1)
	 val numIteration = params.numIteration
	 val stepsize = params.step.toDouble
	 // Sprak Streaming context
	 val sparkConf = new SparkConf().setAppName("doStreamBatchLogisticRegression")
     val ssc = new StreamingContext(sparkConf, Seconds(params.timeIntertoGetData))
     val lines = ssc.socketTextStream(params.IP, params.port.toInt)
     lines.foreachRDD((rdd) => {
      val words = rdd.flatMap(_.split("\n"))
      words.foreach( f =>
        
        if(f.length() >= 10+dimenOfData)
        	{
        		val array = f.split(",")
        		var dataArray = new Array[Double](dimenOfData+1)
        		val label = array(0).toDouble.toInt
        		for (i <- 1 to array.length-1)
        		{	
        		  dataArray(i-1) = array(i).toDouble
        		}
        		//offset
        		dataArray(dimenOfData) = 1.0
        		val oneData = generatePoint(label, dataArray)
        		val tmp = oneData::dataPointArray
        		dataPointArray = tmp
        		numDataInArray = dataPointArray.length
        		if(numDataInArray == numDataInBatch)
        		{//Run Logistic Regression Here
        			println(s"$numDataInBatch points get! Run LR for once")
        			numDataInArray = 0
        			//Run Ridge Regression with this batch of data, the initial parameters are the parameters of the last batch
        			w_stream = runLR(dataPointArray, numIteration, stepsize,w_stream,regPara)
        			println("runLR success")
        			// Clear dataPointArray
        			dataPointArray = List[DataPoint]()
        			
        			//Test after each batch
        			if (!params.testFileName.isEmpty())
        			{
        				val TestDataList = loadData(params.testFileName)
        			    val numData = TestDataList.length
        			    println(s"The number of test data is $numData")
        			    val errorRate = testData(TestDataList, w_stream)
        			    println(s"The errorRate for test data now is $errorRate")
        			}
        		} 
        	}  
      )     
    }       
    )   
    ssc.start()
    ssc.awaitTermination()
    } 	    
    }
def generatePoint(i: Int, data:Array[Double]) = {
    val y = if(i % 2 ==0) -1 else 1
    val x = DenseVector(data)
    DataPoint(x,y)
  }
  def runLR(dataPoints:List[DataPoint], iteration: Int,step:Double, w_int:DenseVector[Double], regPara:Double):DenseVector[Double] ={
    var w = w_int
    val numOfBatch = dataPoints.length.toDouble
    for (i <- 1 to iteration) {  
      var gradient = DenseVector.zeros[Double](dimenOfData+1)
      for (p <- dataPoints) {
        //val scale = (1 / (1 + math.exp(-p.y * (w.dot(p.x)))) - 1) * p.y
        //gradient +=  p.x * scale
        val gra_ls = p.x*(w.dot(p.x) - p.y)
        gradient += gra_ls
      }
      gradient = gradient / numOfBatch + huberGradient(w,epsilon) * regPara
      val norm_g = gradient.norm()
      gradient = gradient.map(fn=> fn/norm_g)   
      w = w - gradient*step
    }
    w
  }
  def loadData(fileName:String):List[DataPoint]={
    var dataPointList = List[DataPoint]()
    for (line <- Source.fromFile(fileName).getLines){
      val oneline = line.split(",")     
      val lable = if(oneline(0).toDouble.toInt%2==0) -1 else 1
      val features = new Array[Double](oneline.length)
      for (i <- 1 to oneline.length-1)
        features(i-1) = oneline(i).toDouble
      features(oneline.length-1) = 1.0
      
      val tem = DataPoint(DenseVector(features), lable):: dataPointList
      dataPointList = tem
    }
    val lengthOfList = dataPointList.length
    //println(s"The lensth of List is $lengthOfList")
    dataPointList
  }
  def testData(dataPointList: List[DataPoint], w:DenseVector[Double]) :Double={
    var truthLabel = List()
    var numError = 0
    for (i <- 0 to dataPointList.length-1){
      val data = dataPointList(i).x
      val lable = dataPointList(i).y
      val pred_lable = if(w.dot(data)>0) 1 else -1
      if(pred_lable != lable)
        numError+=1
    }
    val errorRate = numError.toDouble / dataPointList.length.toDouble 
    errorRate
  }
  def huberGradient(w:DenseVector[Double], epsilon:Double):DenseVector[Double] ={
    val dimen = w.length
    //var huberGra = DenseVector.zeros[Double](dimen)
    val huberGra = w.map(fn=>
      if(fn>epsilon)
        1
      else if(fn< -1*epsilon)
        -1
      else
        fn/epsilon
        )
    huberGra
    
  }
}

