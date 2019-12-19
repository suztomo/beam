/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.dataflow.worker.fn.control;

import static com.fasterxml.jackson.core.JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT;
import static com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_SINGLE_QUOTES;
import static com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonGenerator;
import com.google.api.client.json.JsonParser;
import com.google.api.client.json.JsonToken;
import com.google.api.client.util.Preconditions;
import com.google.api.services.dataflow.model.CounterUpdate;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.SpecMonitoringInfoValidator;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ElementCountMonitoringInfoToCounterUpdateTransformerTest {

  @Rule public final ExpectedException exception = ExpectedException.none();

  @Mock private SpecMonitoringInfoValidator mockSpecValidator;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void tesTransformReturnsNullIfSpecValidationFails() {
    Map<String, NameContext> pcollectionNameMapping = new HashMap<>();
    ElementCountMonitoringInfoToCounterUpdateTransformer testObject =
        new ElementCountMonitoringInfoToCounterUpdateTransformer(
            mockSpecValidator, pcollectionNameMapping);
    Optional<String> error = Optional.of("Error text");
    when(mockSpecValidator.validate(any())).thenReturn(error);
    assertEquals(null, testObject.transform(null));
  }

  @Test
  public void testTransformThrowsIfMonitoringInfoWithWrongUrnPrefixReceived() {
    Map<String, NameContext> pcollectionNameMapping = new HashMap<>();
    MonitoringInfo monitoringInfo =
        MonitoringInfo.newBuilder().setUrn("beam:user:metric:element_count:v1").build();
    ElementCountMonitoringInfoToCounterUpdateTransformer testObject =
        new ElementCountMonitoringInfoToCounterUpdateTransformer(
            mockSpecValidator, pcollectionNameMapping);
    when(mockSpecValidator.validate(any())).thenReturn(Optional.empty());

    exception.expect(RuntimeException.class);
    testObject.transform(monitoringInfo);
  }

  @Test
  public void testTransformReturnsNullIfMonitoringInfoWithUnknownPCollectionLabelPresent() {
    Map<String, NameContext> pcollectionNameMapping = new HashMap<>();
    MonitoringInfo monitoringInfo =
        MonitoringInfo.newBuilder()
            .setUrn("beam:metric:element_count:v1")
            .putLabels(MonitoringInfoConstants.Labels.PCOLLECTION, "anyValue")
            .build();
    ElementCountMonitoringInfoToCounterUpdateTransformer testObject =
        new ElementCountMonitoringInfoToCounterUpdateTransformer(
            mockSpecValidator, pcollectionNameMapping);
    when(mockSpecValidator.validate(any())).thenReturn(Optional.empty());
    assertEquals(null, testObject.transform(monitoringInfo));
  }

  @Test
  public void testTransformReturnsValidCounterUpdateWhenValidMonitoringInfoReceived() {
    Map<String, NameContext> pcollectionNameMapping = new HashMap<>();
    pcollectionNameMapping.put(
        "anyValue",
        NameContext.create("anyStageName", "anyOriginName", "anySystemName", "transformedValue"));

    MonitoringInfo monitoringInfo =
        MonitoringInfo.newBuilder()
            .setUrn("beam:metric:element_count:v1")
            .putLabels(MonitoringInfoConstants.Labels.PCOLLECTION, "anyValue")
            .build();
    ElementCountMonitoringInfoToCounterUpdateTransformer testObject =
        new ElementCountMonitoringInfoToCounterUpdateTransformer(
            mockSpecValidator, pcollectionNameMapping);
    when(mockSpecValidator.validate(any())).thenReturn(Optional.empty());

    CounterUpdate result = testObject.transform(monitoringInfo);
    assertNotEquals(null, result);

    // Comparing JSON is more resilient against dependency upgrades than comparing toString, because
    // the latter is prone to unnecessary test code maintenance upon internal changes such as field
    // ordering and toString implementation.
    assertEqualsOnJson(
        "{cumulative:true, integer:{highBits:0, lowBits:0}, "
            + "nameAndKind:{kind:'SUM', "
            + "name:'transformedValue-ElementCount'}}",
        result);
  }

  // The following field, methods and classes should go to shared test utility class (sdks/testing?)
  private static final NonstrictJacksonFactory jacksonFactory = new NonstrictJacksonFactory();

  public static <T extends GenericJson> void assertEqualsOnJson(String expectedJsonText, T actual) {
    CounterUpdate expected = parse(expectedJsonText, CounterUpdate.class);
    assertEquals(expected, actual);
  }

  public static <T extends GenericJson> T parse(String text, Class<T> clazz) {
    try {
      JsonParser parser = jacksonFactory.createJsonParser(text);
      return parser.parse(clazz);
    } catch (IOException ex) {
      throw new IllegalArgumentException("Could not parse the text as " + clazz, ex);
    }
  }

  public static class NonstrictJacksonFactory extends JsonFactory {

    private final com.fasterxml.jackson.core.JsonFactory factory = new com.fasterxml.jackson.core.JsonFactory();

    public NonstrictJacksonFactory() {
      this.factory.configure(AUTO_CLOSE_JSON_CONTENT, false);
      this.factory.configure(ALLOW_UNQUOTED_FIELD_NAMES, true);
      this.factory.configure(ALLOW_SINGLE_QUOTES, true);
    }

    @Override
    public JsonParser createJsonParser(InputStream inputStream) throws IOException {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public JsonParser createJsonParser(InputStream inputStream, Charset charset)
        throws IOException {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public JsonParser createJsonParser(String value) throws IOException {
      Preconditions.checkNotNull(value);
      return new MyJacksonParser(this, this.factory.createJsonParser(value));
    }

    @Override
    public JsonParser createJsonParser(Reader reader) throws IOException {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public JsonGenerator createJsonGenerator(OutputStream outputStream, Charset charset)
        throws IOException {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public JsonGenerator createJsonGenerator(Writer writer) throws IOException {
      throw new UnsupportedOperationException("Not implemented");
    }


    static JsonToken convert(com.fasterxml.jackson.core.JsonToken token) {
      if (token == null) {
        return null;
      } else {
        switch (token) {
          case END_ARRAY:
            return JsonToken.END_ARRAY;
          case START_ARRAY:
            return JsonToken.START_ARRAY;
          case END_OBJECT:
            return JsonToken.END_OBJECT;
          case START_OBJECT:
            return JsonToken.START_OBJECT;
          case VALUE_FALSE:
            return JsonToken.VALUE_FALSE;
          case VALUE_TRUE:
            return JsonToken.VALUE_TRUE;
          case VALUE_NULL:
            return JsonToken.VALUE_NULL;
          case VALUE_STRING:
            return JsonToken.VALUE_STRING;
          case VALUE_NUMBER_FLOAT:
            return JsonToken.VALUE_NUMBER_FLOAT;
          case VALUE_NUMBER_INT:
            return JsonToken.VALUE_NUMBER_INT;
          case FIELD_NAME:
            return JsonToken.FIELD_NAME;
          default:
            return JsonToken.NOT_AVAILABLE;
        }
      }
    }
  }

  // Google-http-client's JacksonParser is not public
  public static class MyJacksonParser extends JsonParser {

    private final com.fasterxml.jackson.core.JsonParser parser;
    private final NonstrictJacksonFactory factory;

    @Override
    public NonstrictJacksonFactory getFactory() {
      return this.factory;
    }

    MyJacksonParser(NonstrictJacksonFactory factory, com.fasterxml.jackson.core.JsonParser parser) {
      this.factory = factory;
      this.parser = parser;
    }
    @Override
    public void close() throws IOException {
      this.parser.close();
    }
    @Override
    public JsonToken nextToken() throws IOException {
      return NonstrictJacksonFactory.convert(this.parser.nextToken());
    }
    @Override
    public String getCurrentName() throws IOException {
      return this.parser.getCurrentName();
    }
    @Override
    public JsonToken getCurrentToken() {
      return NonstrictJacksonFactory.convert(this.parser.getCurrentToken());
    }
    @Override
    public JsonParser skipChildren() throws IOException {
      this.parser.skipChildren();
      return this;
    }
    @Override
    public String getText() throws IOException {
      return this.parser.getText();
    }
    @Override
    public byte getByteValue() throws IOException {
      return this.parser.getByteValue();
    }
    @Override
    public float getFloatValue() throws IOException {
      return this.parser.getFloatValue();
    }

    @Override
    public int getIntValue() throws IOException {
      return this.parser.getIntValue();
    }

    @Override
    public short getShortValue() throws IOException {
      return this.parser.getShortValue();
    }
    @Override

    public BigInteger getBigIntegerValue() throws IOException {
      return this.parser.getBigIntegerValue();
    }

    @Override
    public BigDecimal getDecimalValue() throws IOException {
      return this.parser.getDecimalValue();
    }

    @Override
    public double getDoubleValue() throws IOException {
      return this.parser.getDoubleValue();
    }

    @Override
    public long getLongValue() throws IOException {
      return this.parser.getLongValue();
    }
  }
}
