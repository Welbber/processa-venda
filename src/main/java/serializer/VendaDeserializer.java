package serializer;

import java.io.IOException;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import model.Venda;
import utils.Log;

public class VendaDeserializer implements Deserializer<Venda> {

	@Override
	public Venda deserialize(String topic, byte[] venda) {
		try {
			return new ObjectMapper().readValue(venda, Venda.class);
		} catch (IOException e) {
			Log.err(VendaDeserializer.class.getName(), e.getMessage());

		}
		return null;
	}
}