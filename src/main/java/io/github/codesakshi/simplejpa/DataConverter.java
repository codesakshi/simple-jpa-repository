package io.github.codesakshi.simplejpa;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.AttributeConverter;
import javax.persistence.Convert;
import javax.persistence.ManyToOne;

import io.github.codesakshi.simplejpa.Annotations.UpdateTimeStamp;

/**
 * 
 * @author anilalps
 *
 */
public class DataConverter {

	/**
	 * 
	 * Inbuilt converter annotations Type
	 *
	 */
	public enum Type {
		BOOLEAN_TO_Y_N,
	}

	@Target(ElementType.FIELD)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConverterType {

		public Type value();
	}

	/**
	 * Convert value from entity to Database column using inbuilt converter annotation
	 * @param inTerm Value to be converted
	 * @param type Inbuilt annotation type
	 * @return Converted value
	 */
	public static Object convertToDatabaseColumn( Object inTerm, Type type) {

		Object value = null;

		switch( type ) {
		case BOOLEAN_TO_Y_N:
			value = convertToDatabaseColumn( inTerm, Y_N.class);
			break;
		default:
			throw new RuntimeException("Converter Not available for :" + type);
		}

		return value;
	}

	/**
	 * Convert value from Database column to entity value using inbuilt converter annotation
	 * @param inTerm Value to be converted
	 * @param type Inbuilt annotation type
	 * @return Converted value
	 */
	public static Object convertToEntityAttribute( Object inTerm, Type type) {

		Object value = null;

		switch( type ) {
		case BOOLEAN_TO_Y_N:
			value = convertToEntityAttribute( inTerm, Y_N.class);
			break;
		default:
			throw new RuntimeException("Converter Not available for :" + type);
		}

		return value;
	}

	/**
	 * Convert value from entity to Database column using AttributeConverter annotation
	 * @param inTerm Value to be converted
	 * @param converter AttributeConverter to be used
	 * @return Converted value
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Object convertToDatabaseColumn( Object inTerm, Class<? extends AttributeConverter> converter) {

		Object value = null;
		try {
			// Create AttributeConverter Object
			AttributeConverter convertObj = converter.newInstance();

			value = convertObj.convertToDatabaseColumn( inTerm);
		}catch( Exception ex ) {
			ex.printStackTrace();
		}

		return value;
	}

	/**
	 * Convert value from Database column to entity value using AttributeConverter annotation
	 * @param inTerm Value to be converted
	 * @param converter AttributeConverter to be used
	 * @return Converted value
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Object convertToEntityAttribute( Object inTerm, Class<? extends AttributeConverter> converter) {

		Object value = null;
		try {

			//Create AttributeConverter Object
			AttributeConverter convertObj = converter.newInstance();

			value = convertObj.convertToEntityAttribute( inTerm);

		}catch( Exception ex ) {
			ex.printStackTrace();
		}

		return value;
	}

	public static class Y_N implements AttributeConverter<Boolean, String>{

		@Override
		public String convertToDatabaseColumn(Boolean attribute) {
			return attribute ? "Y" : "N";
		}

		@Override
		public Boolean convertToEntityAttribute(String dbData) {
			return "Y".equals(dbData);
		}

	};


	@SuppressWarnings("serial")
	private static List<Class<?>> TIME_TYPES = new ArrayList<Class<?>>() {
		{ 
			add(java.sql.Time.class);
			add(java.util.Date.class);
			add(java.time.LocalTime.class);
		}
	};

	@SuppressWarnings("serial")
	private static List<Class<?>> DATE_TYPES = new ArrayList<Class<?>>() {
		{ 
			add(java.sql.Timestamp.class);
			add(java.sql.Date.class);
			add(java.util.Date.class);
			add(java.time.LocalDate.class);
			add(java.time.LocalDateTime.class);
		}
	};

	private static boolean isTimeTypes( Class<?> sourceType,  Class<?> targetType ) {

		boolean bret = false;

		for( Class<?> classType : TIME_TYPES ) {

			if( classType.isAssignableFrom(sourceType) ) {
				bret = true;
				break;
			}
		}

		if( bret ) {

			bret = false;

			for( Class<?> classType : TIME_TYPES ) {

				if( classType.isAssignableFrom(targetType) ) {
					bret = true;
					break;
				}
			}
		}

		return bret;

	}

	private static boolean isDateTypes( Class<?> sourceType,  Class<?> targetType ) {

		boolean bret = false;

		for( Class<?> classType : DATE_TYPES ) {

			if( classType.isAssignableFrom(sourceType) ) {
				bret = true;
				break;
			}
		}

		if( bret ) {

			bret = false;

			for( Class<?> classType : DATE_TYPES ) {

				if( classType.isAssignableFrom(targetType) ) {
					bret = true;
					break;
				}
			}
		}

		return bret;
	}

	/**
	 * Convert value to SQL type
	 * @param field - field of the entity value
	 * @param value entity value
	 * @return Converted value
	 * @throws Exception If the conversion fails
	 */
	public static Object convertValueToSql( Field field, Object value ) throws Exception {

		Object outObject = value;

		ManyToOne manyToOne = field.getAnnotation(ManyToOne.class); 

		if( null == manyToOne ) {

			// Check and Convert UpdateTimeStamp

			UpdateTimeStamp updateTime = field.getAnnotation(UpdateTimeStamp.class);

			if( null != updateTime ) {

				outObject = getTimeStampValueInSqlType( field.getType() );

			}else {

				//Convert to sql types
				outObject = convertToSqlTypes( field, value );
			}

		}

		return outObject;
	}

	private static Object getTimeStampValueInSqlType( Class<?> targetType ) throws Exception {

		Object value = null;

		if( isDateTypes( java.util.Date.class, targetType )) {

			value = convertDate( new Date(), targetType);

		}else if( isTimeTypes( java.util.Date.class, targetType ) ) {

			value = convertTime( new Date(), targetType);

		}else {

			throw new Exception( " Target Type : " + targetType.getName() + " is not comaptible with Date or Time." );
		}

		return value;
	}

	/**
	 * Convert Date from one type to another type
	 * 
	 * Dates
	 * 
	 *  java.sql.Timestamp
	 *  java.sql.Date
	 *  java.util.Date
	 *  java.time.LocalDate
	 *  java.time.LocalDateTime
	 *  
	 * @param inItem Value to be converted
	 * @param targetType Class of target Type
	 * @return Converted value
	 * @throws Exception If the conversion fails
	 */
	private static Object convertDate( Object inItem, Class<?> targetType ) throws Exception {

		Object value = null;

		if( null != inItem ) {

			Class<?> sourceType = inItem.getClass();

			if( sourceType.equals(targetType) ) {

				value = inItem;

			}else {

				// Date Conversion
				// Convert source to Instant.				
				Instant instant = null;

				if( java.sql.Timestamp.class.isAssignableFrom(sourceType) ) {

					instant = Instant.ofEpochMilli( ((java.sql.Timestamp)inItem).getTime() );

				} else if( java.sql.Date.class.isAssignableFrom(sourceType) ) {

					instant = Instant.ofEpochMilli( ((java.sql.Date)inItem).getTime() );

				}else if( java.util.Date.class.isAssignableFrom(sourceType) ) {			

					instant = Instant.ofEpochMilli( ((java.util.Date)inItem).getTime() );

				}else if( LocalDate.class.isAssignableFrom(sourceType) ) {

					instant = ((LocalDate)inItem)
							.atStartOfDay(ZoneId.systemDefault())
							.toInstant();					

				}else if( LocalDateTime.class.isAssignableFrom(sourceType) ) {

					instant = ((LocalDateTime)inItem)
							.atZone(ZoneId.systemDefault())
							.toInstant();
				}else {
					throw new Exception( " Argument Type : " + sourceType.getName() + " is not a valid Date Type." );
				}

				// Convert to Target Date Type
				if( java.sql.Timestamp.class.isAssignableFrom(targetType) ) {

					value = java.sql.Timestamp.from(instant);

				} else if( java.sql.Date.class.isAssignableFrom(targetType) ) {

					value = new java.sql.Date(instant.toEpochMilli());

				} else if( java.util.Date.class.isAssignableFrom(targetType) ) {

					value = java.util.Date.from(instant);

				}else if( LocalDate.class.isAssignableFrom(targetType) ) {

					value = LocalDate.from(instant);

				}else if( LocalDateTime.class.isAssignableFrom(targetType) ) {

					value = LocalDateTime.from(instant);

				}else {
					throw new Exception( " Target Type : " + targetType.getName() + " is not a valid Date Type." );
				}
			}			
		}

		return value;
	}

	/**
	 * 	Convert Time from one type to another type
	 * Time
	 *  java.sql.Time
	 *  java.util.Date
	 *  java.time.LocalTime
	 * 
	 * @param inItem Value to be converted
	 * @param targetType Class of target Type
	 * @return Converted value
	 * @throws Exception If the conversion fails
	 */
	private static Object convertTime( Object inItem, Class<?> targetType ) throws Exception {

		Object value = null;

		if( null != inItem ) {

			Class<?> sourceType = inItem.getClass();

			if( sourceType.equals(targetType) ) {

				value = inItem;

			}else {

				// Convert source to Instant.				
				Instant instant = null;
				if( java.sql.Time.class.isAssignableFrom(sourceType) ) {

					instant = Instant.ofEpochMilli( ((java.sql.Time)inItem).getTime() );

				} else if( java.util.Date.class.isAssignableFrom(sourceType) ) {

					instant = Instant.ofEpochMilli( ((java.util.Date)inItem).getTime() );

				} else if( LocalTime.class.isAssignableFrom(sourceType) ) {

					instant = ((LocalTime)inItem)
							.atDate(LocalDate.of(0, 0, 0))
							.atZone(ZoneId.systemDefault())
							.toInstant();
				}else {
					throw new Exception( " Argument Type : " + sourceType.getName() + " is not a valid Time Type." );
				}

				// Convert Instant Target Time Type
				if( java.sql.Time.class.isAssignableFrom(targetType) ) {

					value = java.sql.Time.from(instant);

				} else if( java.util.Date.class.isAssignableFrom(targetType) ) {

					value = java.util.Date.from(instant);

				}else if( LocalTime.class.isAssignableFrom(targetType) ) {

					value = LocalTime.from(instant);

				}else {
					throw new Exception( " Target Type : " + targetType.getName() + " is not a valid Time Type." );
				}
			}
		}

		return value;
	}

	/**
	 * Convert value from one type to another type
	 * @param inItem - Value to be converted
	 * @param targetType - Class of target type
	 * @return Converted value
	 * @throws Exception If the conversion fails
	 */
	public static Object convertToCompatibleType( Object inItem, Class<?> targetType ) throws Exception {

		Object value = null;

		if( null != inItem ) {

			Class<?> sourceType = inItem.getClass();

			if( targetType.isAssignableFrom( sourceType ) ) {

				value = targetType.cast( inItem );

			}else {

				if( isDateTypes( sourceType, targetType )) {

					value = convertDate( inItem, targetType);

				}else if( isTimeTypes(sourceType, targetType ) ) {

					value = convertTime( inItem, targetType);

				}else if( sourceType == BigDecimal.class ) {

					BigDecimal inValue = (BigDecimal)inItem;

					if( targetType == Long.class){

						value = inValue.longValue();

					}else if( targetType == Integer.class){

						value = inValue.intValueExact();

					}else if( targetType == BigInteger.class){

						value = inValue.toBigInteger();

					} else if( targetType == Boolean.class){

						value =  0 != inValue.intValueExact();

					} else if( targetType == Float.class){

						value = inValue.floatValue();
					}

				} else if( sourceType == BigInteger.class ) {

					BigInteger inValue = (BigInteger)inItem;

					if( targetType == Long.class){

						value = inValue.longValue();

					}else if( targetType == Integer.class){

						value = inValue.intValueExact();

					}else if( targetType == BigDecimal.class){

						value = new BigDecimal( inValue);

					} else if( targetType == Boolean.class){

						value =  0 != inValue.intValueExact();

					} else if( targetType == Float.class){

						value = inValue.floatValue();
					}

				} else if( sourceType == Integer.class ) {

					Integer inValue = (Integer)inItem;

					if( targetType == Float.class){

						value = new Float( inValue );
					} else if( targetType == Long.class ) {

						value = new Long( inValue );
					}

				} else if( sourceType == Long.class ) {

					Long inValue = (Long)inItem;

					if( targetType == Float.class){

						value = new Float( inValue );
					} else if( targetType == Integer.class ) {

						value = inValue.intValue();
					}		

					// Check and convert to String
				}else if( targetType == String.class ) {

					value = String.valueOf( inItem );

				}
			}

			if( null == value ) {
				// Conversion logic is not available

				throw new SQLException( "Could not convert Data base type "  + sourceType.getName() 
				+ " to Entity Type : " + targetType.getName() );
			}
		}

		return value;
	}

	/**
	 * Convert value to SQL type
	 * 
	 * @param field Field of value in entity 
	 * @param inItem Value to be converted
	 * @return Converted value
	 * @throws Exception If the conversion fails
	 */
	@SuppressWarnings("unchecked")
	private static Object convertToSqlTypes( Field field, Object inItem ) throws Exception {

		Object value = inItem;

		if( null != inItem ) {

			Class<?> sourceType = field.getType();

			Convert convert = field.getAnnotation( Convert.class );

			ConverterType converterType = field.getAnnotation( ConverterType.class );

			if( null != convert && ! convert.disableConversion() ) {

				if( ! convert.converter().isAssignableFrom( AttributeConverter.class ) ) {

					throw new Exception( " Convert is not a AttributeConverter type " );
				}

				value = convertToDatabaseColumn( inItem, convert.converter());

			}else if( null != converterType ) {

				value = convertToDatabaseColumn( inItem, converterType.value() );

			}else if (sourceType == BigInteger.class) {

				// Big Integer is not supported in Database column Type
				// So we will be using Long Type instead
				value = ((BigInteger)inItem).longValue();

			}else if (sourceType == Date.class) {

				value = new  java.sql.Date( ((Date) inItem).getTime() );

			} else if (sourceType == LocalDate.class) {

				value = java.sql.Date.valueOf( ((LocalDate) inItem) );

			} else if (sourceType == LocalTime.class) {

				value = java.sql.Time.valueOf( ((LocalTime) inItem) );

			} else if (sourceType == LocalDateTime.class) {

				value = java.sql.Timestamp.valueOf( ((LocalDateTime) inItem) );
			}
		}

		return value;		
	}

}
