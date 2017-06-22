//Another way for inferring type  of data in csv.
//This approach will be more efficient to creating generic types. Users only provide particularly implementation of
//trait CSVType and that's all
trait CSVType {
  //can add some useful methods
}

case class Person(name: String, age: String, birthday: String, gender: String) extends CSVType


