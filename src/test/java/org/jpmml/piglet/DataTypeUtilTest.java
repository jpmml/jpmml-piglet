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

import org.dmg.pmml.DataType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataTypeUtilTest {

	@Test
	public void isCompatible(){
		assertTrue(DataTypeUtil.isCompatible(DataType.INTEGER, DataType.STRING));
		assertTrue(DataTypeUtil.isCompatible(DataType.INTEGER, DataType.INTEGER));
		assertFalse(DataTypeUtil.isCompatible(DataType.INTEGER, DataType.FLOAT));
		assertFalse(DataTypeUtil.isCompatible(DataType.INTEGER, DataType.DOUBLE));
		assertTrue(DataTypeUtil.isCompatible(DataType.INTEGER, DataType.BOOLEAN));
	}

	@Test
	public void formatDataType(){
		assertEquals(org.apache.pig.data.DataType.INTEGER, DataTypeUtil.formatDataType(DataType.INTEGER));
	}

	@Test
	public void parseDataType(){
		assertEquals(DataType.INTEGER, DataTypeUtil.parseDataType(org.apache.pig.data.DataType.INTEGER));

		try {
			DataTypeUtil.parseDataType(org.apache.pig.data.DataType.BIGINTEGER);

			fail();
		} catch(IllegalArgumentException iae){
			// Ignored
		}
	}
}