package org.ivan.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ClaveFechaProceso implements WritableComparable<ClaveFechaProceso> {
	
	private Text fecha;
	private Text proceso;

	public ClaveFechaProceso() {
		fecha = new Text();
		proceso = new Text();
	}

	public ClaveFechaProceso(Text fecha, Text proceso) {
		this.fecha = fecha;
		this.proceso = proceso;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fecha == null) ? 0 : fecha.hashCode());
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
		ClaveFechaProceso other = (ClaveFechaProceso) obj;
		if (fecha == null) {
			if (other.fecha != null)
				return false;
		} else if (!fecha.equals(other.fecha))
			return false;
		if (proceso == null) {
			if (other.proceso != null)
				return false;
		} else if (!proceso.equals(other.proceso))
			return false;
		return true;
	}

	public Text getFecha() {
		return fecha;
	}

	public void setFecha(Text fecha) {
		this.fecha = fecha;
	}

	public Text getProceso() {
		return proceso;
	}

	public void setProceso(Text proceso) {
		this.proceso = proceso;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		fecha.readFields(in);
		proceso.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		fecha.write(out);
		proceso.write(out);
	}

	@Override
	public int compareTo(ClaveFechaProceso o) {
		
		int cmp = fecha.compareTo(o.getFecha());
		if (cmp != 0) {
			return cmp;
		} else
			return proceso.compareTo(o.getProceso());
	}

	@Override
	public String toString() {
		return "claveFechaProceso [fecha=" + fecha + ", proceso=" + proceso + "]";
	}

}
