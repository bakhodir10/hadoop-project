package org.myorg;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class CustomMapWritable extends MapWritable{
	
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		for(Writable key: this.keySet()){
			b.append("(").append(key).append(":").append(this.get(key) + "), ");
		}
		return b.toString();
	}
}
