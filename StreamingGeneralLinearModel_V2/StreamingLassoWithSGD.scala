package org.apache.spark.mllib.regression

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.{Vector, Vectors}

@Experimental
class StreamingLassoWithSGD (
    private var stepSize: Double,
    private var numIterations: Int,
    private var miniBatchFraction: Double,
    private var regParam: Double,
    private var initialWeights: Vector)
  extends StreamingGeneralizedLinearAlgorithm[
    LassoModel, LassoWithSGD]  with Serializable {

  def this() = this(0.1, 50, 1.0, 0.1,null)
 
 val algorithm = new LassoWithSGD(stepSize, numIterations, regParam, miniBatchFraction)
  

  var model = algorithm.createModel(initialWeights, 0.0)


  /** Set the step size for gradient descent. Default: 0.1. */
  def setStepSize(stepSize: Double): this.type = {
    this.algorithm.optimizer.setStepSize(stepSize)
    this
  }

  /** Set the number of iterations of gradient descent to run per update. Default: 50. */
  def setNumIterations(numIterations: Int): this.type = {
    this.algorithm.optimizer.setNumIterations(numIterations)
    this
  }

  /** Set the fraction of each batch to use for updates. Default: 1.0. */
  def setMiniBatchFraction(miniBatchFraction: Double): this.type = {
    this.algorithm.optimizer.setMiniBatchFraction(miniBatchFraction)
    this
  }

  /** Set the initial weights. Default: [0.0, 0.0]. */
  def setInitialWeights(initialWeights: Vector): this.type = {
    this.model = algorithm.createModel(initialWeights, 0.0)
    this
  }

}
