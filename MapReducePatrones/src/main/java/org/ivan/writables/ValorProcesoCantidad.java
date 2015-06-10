package org.ivan.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ValorProcesoCantidad implements Writable {
	
	private Text proceso;
	private LongWritable cantidad;
	
	public ValorProcesoCantidad(){
		proceso = new Text();
		cantidad = new LongWritable();
	}

	public ValorProcesoCantidad(Text proceso, LongWritable cantidad) {
		this.proceso = proceso;
		this.cantidad = cantidad;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cantidad == null) ? 0 : cantidad.hashCode());
		result = prime * result + ((proceso == null) ? 0 : proceso.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ValorProcesoCantidad other = (ValorProcesoCantidad) obj;
		if (cantidad == null) {
			if (other.cantidad != null)
				return false;
		} else if (!cantidad.equals(other.cantidad))
			return false;
		if (proceso == null) {
			if (other.proceso != null)
				return false;
		} else if (!proceso.equals(other.proceso))
			return false;
		return true;
	}

	public Text getProceso() {
		return proceso;
	}

	public void setProceso(Text proceso) {
		this.proceso = proceso;
	}

	public LongWritable getCantidad() {
		return cantidad;
	}

	public void setCantidad(LongWritable cantidad) {
		this.cantidad = cantidad;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		proceso.readFields(in);
		cantidad.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		proceso.write(out);
		cantidad.write(out);
	}

	@Override
	public String toString() {
		return proceso + ":" + cantidad + ", ";
	}

}
