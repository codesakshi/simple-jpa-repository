package io.github.codesakshi.simplejpa.repository;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 
 * @author anilalps
 *
 */
public class Annotations {
/**
 * Specify UpdateTimeStamp Annotation
 */
	@Target(ElementType.FIELD)
	@Retention( RetentionPolicy.RUNTIME)
	public @interface UpdateTimeStamp{
		
	}
}
