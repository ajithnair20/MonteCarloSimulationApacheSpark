import org.scalatest.FunSuite

class SimulationHelperTest  extends FunSuite {

    test("Profit Calculation Test"){
        val pricesList = List(15.0,12.0,13.0,16.0,19.0,12.0,11.0)
        val profit = pricesList.zip(pricesList.scan(Double.MaxValue)(math.min).tail).map(p => p._1 - p._2).foldLeft(0.0)(math.max)
        assert(profit == 7.0)
    }
}
