package com.zegelin.prometheus.exposition;

import com.google.common.base.Stopwatch;
import com.google.common.escape.CharEscaperBuilder;
import com.google.common.escape.Escaper;
import com.zegelin.prometheus.domain.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class JsonFormatChunkedInput implements ChunkedInput<ByteBuf> {
    private enum State {
        HEADER,
        METRIC_FAMILY,
        METRIC,
        FOOTER,
        EOF
    }

    private enum MetricFamilyType {
        GAUGE,
        COUNTER,
        HISTOGRAM,
        SUMMARY,
        UNTYPED;

        void write(final ExpositionSink sink) {
            Json.writeAsciiString(sink, name());
        }
    }

    private static final Escaper JSON_STRING_ESCAPER = new CharEscaperBuilder()
            .addEscape('"', "\\\"")
            .addEscape('\\', "\\\\")
            .addEscape('/', "\\/")
            .addEscape('\b', "\\b")
            .addEscape('\f', "\\f")
            .addEscape('\n', "\\n")
            .addEscape('\r', "\\r")
            .addEscape('\t', "\\t")
            .toEscaper();


    private final Iterator<MetricFamily> metricFamilyIterator;

    private final Instant timestamp;
    private final Labels globalLabels;
    private final boolean includeHelp;

    private State state = State.HEADER;
    private MetricFamilyWriter metricFamilyWriter;

    private int metricFamilyCount = 0;
    private int metricCount = 0;

    private final Stopwatch stopwatch = Stopwatch.createUnstarted();


    public JsonFormatChunkedInput(final Stream<MetricFamily> metricFamilies, final Instant timestamp, final Labels globalLabels, final boolean includeHelp) {
        this.metricFamilyIterator = metricFamilies.iterator();
        this.timestamp = timestamp;
        this.globalLabels = globalLabels;
        this.includeHelp = includeHelp;
    }

    @Override
    public boolean isEndOfInput() throws Exception {
        return state == State.EOF;
    }

    @Override
    public void close() throws Exception {
    }


    static final class Json {
        enum Token {
            OBJECT_START('{'),
            OBJECT_END('}'),
            ARRAY_START('['),
            ARRAY_END(']'),
            DOUBLE_QUOTE('"'),
            COMMA(','),
            COLON(':');

            final byte encoded;

            Token(final char c) {
                this.encoded = (byte) c;
            }

            void write(final ExpositionSink sink) {
                sink.writeByte(encoded);
            }
        }

        private static void writeNull(final ExpositionSink sink) {
            sink.writeAscii("null");
        }

        private static void writeAsciiString(final ExpositionSink sink, final String key) {
            Token.DOUBLE_QUOTE.write(sink);
            sink.writeAscii(key);
            Token.DOUBLE_QUOTE.write(sink);
        }

        private static void writeUtf8String(final ExpositionSink sink, final String key) {
            Token.DOUBLE_QUOTE.write(sink);
            sink.writeUtf8(key);
            Token.DOUBLE_QUOTE.write(sink);
        }

        private static void writeObjectKey(final ExpositionSink sink, final String key) {
            writeAsciiString(sink, key);
            Token.COLON.write(sink);
        }

        private static void writeFloat(final ExpositionSink sink, final float f) {
            if (Float.isNaN(f)) {
                writeAsciiString(sink, "NaN");
                return;
            }

            if (Float.isInfinite(f)) {
                writeAsciiString(sink, (f < 0 ? "-Inf" : "+Inf"));
                return;
            }

            sink.writeFloat(f);
        }

        private static void writeLong(final ExpositionSink sink, final long l) {
            sink.writeAscii(Long.toString(l));
        }
    }

    public static ByteBuf formatLabels(final Map<String, String> labels) {
        final NettyExpositionSink sink = new NettyExpositionSink(Unpooled.buffer());

        Json.Token.OBJECT_START.write(sink);

        final Iterator<Map.Entry<String, String>> labelsIterator = labels.entrySet().iterator();

        while (labelsIterator.hasNext()) {
            final Map.Entry<String, String> label = labelsIterator.next();

            Json.writeObjectKey(sink, label.getKey());
            Json.writeUtf8String(sink, JSON_STRING_ESCAPER.escape(label.getValue()));

            if (labelsIterator.hasNext()) {
                Json.Token.COMMA.write(sink);
            }
        }

        Json.Token.OBJECT_END.write(sink);

        return sink.getBuffer();
    }


    class MetricFamilyWriter {
        private final Consumer<ExpositionSink> headerWriter;
        private final Function<ExpositionSink, Boolean> metricWriter;

        class HeaderVisitor implements MetricFamilyVisitor<Consumer<ExpositionSink>> {
            private void writeFamilyHeader(final MetricFamily<?> metricFamily, final ExpositionSink sink, final MetricFamilyType type) {
                Json.writeObjectKey(sink, metricFamily.name);

                Json.Token.OBJECT_START.write(sink);

                Json.writeObjectKey(sink, "type");
                type.write(sink);

                if (includeHelp && metricFamily.help != null) {
                    Json.Token.COMMA.write(sink);

                    Json.writeObjectKey(sink, "help");
                    Json.writeUtf8String(sink, JSON_STRING_ESCAPER.escape(metricFamily.help));
                }

                Json.Token.COMMA.write(sink);

                Json.writeObjectKey(sink, "metrics");
                Json.Token.ARRAY_START.write(sink);
            }

            private Consumer<ExpositionSink> forType(final MetricFamily<?> metricFamily, final MetricFamilyType type) {
                return (buffer) -> writeFamilyHeader(metricFamily, buffer, type);
            }

            @Override
            public Consumer<ExpositionSink> visit(final CounterMetricFamily metricFamily) {
                return forType(metricFamily, MetricFamilyType.COUNTER);
            }

            @Override
            public Consumer<ExpositionSink> visit(final GaugeMetricFamily metricFamily) {
                return forType(metricFamily, MetricFamilyType.GAUGE);
            }

            @Override
            public Consumer<ExpositionSink> visit(final SummaryMetricFamily metricFamily) {
                return forType(metricFamily, MetricFamilyType.SUMMARY);
            }

            @Override
            public Consumer<ExpositionSink> visit(final HistogramMetricFamily metricFamily) {
                return forType(metricFamily, MetricFamilyType.HISTOGRAM);
            }

            @Override
            public Consumer<ExpositionSink> visit(final UntypedMetricFamily metricFamily) {
                return forType(metricFamily, MetricFamilyType.UNTYPED);
            }
        }

        class MetricVisitor implements MetricFamilyVisitor<Function<ExpositionSink, Boolean>> {
            private <T extends Metric> Function<ExpositionSink, Boolean> metricWriter(final MetricFamily<T> metricFamily, final BiConsumer<T, ExpositionSink> valueWriter) {
                final Iterator<T> metricIterator = metricFamily.metrics().iterator();

                return (buffer) -> {
                    if (metricIterator.hasNext()) {
                        final T metric = metricIterator.next();

                        Json.Token.OBJECT_START.write(buffer);
                        Json.writeObjectKey(buffer, "labels");
                        if (metric.labels != null) {
                            buffer.writeBytes(metric.labels.asJSONFormatUTF8EncodedByteBuf().nioBuffer());
                        } else {
                            Json.writeNull(buffer);
                        }

                        Json.Token.COMMA.write(buffer);

                        Json.writeObjectKey(buffer, "value");
                        valueWriter.accept(metric, buffer);

                        Json.Token.OBJECT_END.write(buffer);

                        if (metricIterator.hasNext()) {
                            Json.Token.COMMA.write(buffer);
                        }

                        return true;
                    }

                    return false;
                };
            }

            @Override
            public Function<ExpositionSink, Boolean> visit(final CounterMetricFamily metricFamily) {
                return metricWriter(metricFamily, (counter, buffer) -> {
                    Json.writeFloat(buffer, counter.value);
                });
            }

            @Override
            public Function<ExpositionSink, Boolean> visit(final GaugeMetricFamily metricFamily) {
                return metricWriter(metricFamily, (gauge, buffer) -> {
                    Json.writeFloat(buffer, gauge.value);
                });
            }

            private void writeSumAndCount(final ExpositionSink sink, final float sum, final float count) {
                Json.writeObjectKey(sink, "sum");
                Json.writeFloat(sink, sum);

                Json.Token.COMMA.write(sink);

                Json.writeObjectKey(sink, "count");
                Json.writeFloat(sink, count);
            }

            private void writeIntervals(final ExpositionSink sink, final Stream<Interval> intervals) {
                Json.Token.OBJECT_START.write(sink);

                final Iterator<Interval> iterator = intervals.iterator();

                while (iterator.hasNext()) {
                    final Interval interval = iterator.next();

                    Json.writeObjectKey(sink, interval.quantile.toString());
                    Json.writeFloat(sink, interval.value);

                    if (iterator.hasNext()) {
                        Json.Token.COMMA.write(sink);
                    }
                }

                Json.Token.OBJECT_END.write(sink);
            }

            @Override
            public Function<ExpositionSink, Boolean> visit(final SummaryMetricFamily metricFamily) {
                return metricWriter(metricFamily, (summary, buffer) -> {
                    Json.Token.OBJECT_START.write(buffer);

                    writeSumAndCount(buffer, summary.sum, summary.count);

                    Json.Token.COMMA.write(buffer);

                    Json.writeObjectKey(buffer, "quantiles");
                    writeIntervals(buffer, summary.quantiles);

                    Json.Token.OBJECT_END.write(buffer);
                });
            }

            @Override
            public Function<ExpositionSink, Boolean> visit(final HistogramMetricFamily metricFamily) {
                return metricWriter(metricFamily, (histogram, buffer) -> {
                    Json.Token.OBJECT_START.write(buffer);

                    writeSumAndCount(buffer, histogram.sum, histogram.count);

                    Json.Token.COMMA.write(buffer);

                    Json.writeObjectKey(buffer, "buckets");
                    writeIntervals(buffer, histogram.buckets);

                    Json.Token.OBJECT_END.write(buffer);
                });
            }

            @Override
            public Function<ExpositionSink, Boolean> visit(final UntypedMetricFamily metricFamily) {
                return metricWriter(metricFamily, (untyped, buffer) -> {
                    Json.writeFloat(buffer, untyped.value);
                });
            }
        }

        MetricFamilyWriter(final MetricFamily<?> metricFamily) {
            this.headerWriter = metricFamily.accept(new HeaderVisitor());
            this.metricWriter = metricFamily.accept(new MetricVisitor());
        }

        void writeFamilyHeader(final ExpositionSink sink) {
            this.headerWriter.accept(sink);
        }

        void writeFamilyFooter(final ExpositionSink sink) {
            Json.Token.ARRAY_END.write(sink);
            Json.Token.OBJECT_END.write(sink);
        }

        boolean writeMetric(final ExpositionSink sink) {
            return this.metricWriter.apply(sink);
        }
    }

    private void writeStatistics(final ExpositionSink sink) {
        Json.Token.OBJECT_START.write(sink);

        Json.writeObjectKey(sink, "expositionTime");
        Json.writeLong(sink, stopwatch.elapsed(TimeUnit.MILLISECONDS));

        Json.Token.COMMA.write(sink);

        Json.writeObjectKey(sink, "metricFamilyCount");
        Json.writeLong(sink, metricFamilyCount);

        Json.Token.COMMA.write(sink);

        Json.writeObjectKey(sink, "metricCount");
        Json.writeLong(sink, metricCount);

        Json.Token.OBJECT_END.write(sink);
    }

    private void nextSlice(final ExpositionSink sink) {
        switch (state) {
            case HEADER:
                stopwatch.start();

                Json.Token.OBJECT_START.write(sink);

                Json.writeObjectKey(sink, "timestamp");
                Json.writeLong(sink, timestamp.toEpochMilli());

                Json.Token.COMMA.write(sink);

                Json.writeObjectKey(sink, "globalLabels");
                sink.writeBytes(globalLabels.asJSONFormatUTF8EncodedByteBuf().nioBuffer());

                Json.Token.COMMA.write(sink);

                Json.writeObjectKey(sink, "metricFamilies");
                Json.Token.OBJECT_START.write(sink);

                state = State.METRIC_FAMILY;
                return;

            case METRIC_FAMILY:
                if (!metricFamilyIterator.hasNext()) {
                    state = State.FOOTER;
                    return;
                }

                metricFamilyCount++;

                final MetricFamily<?> metricFamily = metricFamilyIterator.next();

                metricFamilyWriter = new MetricFamilyWriter(metricFamily);

                metricFamilyWriter.writeFamilyHeader(sink);

                state = State.METRIC;
                return;

            case METRIC:
                if (!metricFamilyWriter.writeMetric(sink)) {
                    metricFamilyWriter.writeFamilyFooter(sink);

                    if (metricFamilyIterator.hasNext()) {
                        Json.Token.COMMA.write(sink);
                    }

                    state = State.METRIC_FAMILY;
                    return;
                }

                metricCount++;

                return;

            case FOOTER:
                stopwatch.stop();

                Json.Token.OBJECT_END.write(sink); // end of "metricFamilies"

                Json.Token.COMMA.write(sink);

                Json.writeObjectKey(sink, "statistics");
                writeStatistics(sink);

                Json.Token.OBJECT_END.write(sink); // end of main object

                state = State.EOF;
                return;

            case EOF:
                return;

            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public ByteBuf readChunk(final ChannelHandlerContext ctx) throws Exception {
        final ByteBuf chunkBuffer = ctx.alloc().buffer(1024 * 1024 * 5);

        // add slices till we hit the chunk size (or slightly over it), or hit EOF
        while (chunkBuffer.readableBytes() < 1024 * 1024 && state != State.EOF) {
            nextSlice(new NettyExpositionSink(chunkBuffer));
        }

        return chunkBuffer;
    }
}
