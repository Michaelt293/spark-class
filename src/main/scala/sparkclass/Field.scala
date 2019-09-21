package sparkclass

final case class Field(
    fieldName: String,
    primitiveType: PrimitiveType,
    required: Boolean,
    dataDescription: Option[String]
) {
  def showPrimitiveType: String =
    if (required) primitiveType.toString else s"Option[$primitiveType]"
}
