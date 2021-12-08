package serializer;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import model.Venda;
import utils.Log;

public class VendaSerializer implements Serializer<Venda> {

	@Override
	public byte[] serialize(String topic, Venda venda) {

		try {
			return new ObjectMapper().writeValueAsBytes(venda);
		} catch (JsonProcessingException e) {
			Log.err(VendaSerializer.class.getName(), e.getMessage());
		}
		return null;
	}
}
