/*
 * Copyright (c) 2016 Villu Ruusmann
 *
 * This file is part of JPMML-Piglet
 *
 * JPMML-Piglet is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * JPMML-Piglet is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with JPMML-Piglet.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.jpmml.piglet;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Set;

import org.dmg.pmml.DataType;

public class DataTypeUtil {

	private DataTypeUtil(){
	}

	static
	public boolean isCompatible(DataType expectedDataType, DataType actualDataType){

		if(expectedDataType == actualDataType){
			return true;
		} // End if

		// A string representation of a value can be parsed to any other representation
		if(DataType.STRING == actualDataType){
			return true;
		}

		Set<DataType> compatibleTypes = DataTypeUtil.compatibleTypes.get(expectedDataType);
		if(compatibleTypes != null){
			return compatibleTypes.contains(actualDataType);
		}

		return false;
	}

	static
	public byte formatDataType(DataType dataType){

		switch(dataType){
			case STRING:
				return org.apache.pig.data.DataType.CHARARRAY;
			case INTEGER:
				return org.apache.pig.data.DataType.INTEGER;
			case FLOAT:
				return org.apache.pig.data.DataType.FLOAT;
			case DOUBLE:
				return org.apache.pig.data.DataType.DOUBLE;
			case BOOLEAN:
				return org.apache.pig.data.DataType.BOOLEAN;
			default:
				throw new IllegalArgumentException("PMML data type " + dataType + " does not have a corresponding Apache Pig data type");
		}
	}

	static
	public DataType parseDataType(byte type){

		switch(type){
			case org.apache.pig.data.DataType.CHARARRAY:
				return DataType.STRING;
			case org.apache.pig.data.DataType.INTEGER:
			case org.apache.pig.data.DataType.LONG:
				return DataType.INTEGER;
			case org.apache.pig.data.DataType.FLOAT:
				return DataType.FLOAT;
			case org.apache.pig.data.DataType.DOUBLE:
				return DataType.DOUBLE;
			case org.apache.pig.data.DataType.BOOLEAN:
				return DataType.BOOLEAN;
			default:
				throw new IllegalArgumentException("Apache Pig data type " + org.apache.pig.data.DataType.findTypeName(type) + " does not have a corresponding PMML data type");
		}
	}

	private static final EnumMap<DataType, Set<DataType>> compatibleTypes = new EnumMap<>(DataType.class);

	static {
		compatibleTypes.put(DataType.INTEGER, EnumSet.of(DataType.BOOLEAN));
		compatibleTypes.put(DataType.FLOAT, EnumSet.of(DataType.INTEGER, DataType.DOUBLE, DataType.BOOLEAN));
		compatibleTypes.put(DataType.DOUBLE, EnumSet.of(DataType.INTEGER, DataType.FLOAT, DataType.BOOLEAN));
	}
}