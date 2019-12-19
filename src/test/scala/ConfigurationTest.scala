import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class ConfigurationTest extends FunSuite {

  //test if configuration file is loaded correctly
  test("Configuration File is loaded correctly"){
    val config = ConfigFactory.load("Application.conf")
    assert(config!=null)

    //test if values are loaded correctly from configuration file
    val Samples = config.getString("Samples")
    assert( Samples == "5")
  }
}
