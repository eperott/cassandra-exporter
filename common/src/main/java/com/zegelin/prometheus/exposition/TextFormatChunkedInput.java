package com.zegelin.prometheus.exposition;

import com.google.common.base.Stopwatch;
import com.google.common.escape.CharEscaperBuilder;
import com.google.common.escape.Escaper;
import com.zegelin.netty.Resources;
import com.zegelin.prometheus.domain.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class TextFormatChunkedInput implements ChunkedInput<ByteBuf> {
    private enum State {
        BANNER,
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

        private final String encoded;

        MetricFamilyType() {
            encoded = this.name().toLowerCase();
        }

        void write(final ExpositionSink sink) {
            sink.writeAscii(encoded);
        }
    }

    private static Escaper HELP_STRING_ESCAPER = new CharEscaperBuilder()
            .addEscape('\\', "\\\\")
            .addEscape('\n', "\\n")
            .toEscaper();

    private static Escaper LABEL_VALUE_ESCAPER = new CharEscaperBuilder()
            .addEscape('\\', "\\\\")
            .addEscape('\n', "\\n")
            .addEscape('"', "\\\"")
            .toEscaper();

    private static final ByteBuf BANNER = Resources.asByteBuf(TextFormatChunkedInput.class, "banner.txt");

    private final Iterator<MetricFamily> metricFamilyIterator;

    private final String timestamp;
    private final Labels globalLabels;
    private final boolean includeHelp;

    private State state = State.BANNER;
    private MetricFamilyWriter metricFamilyWriter;

    private int metricFamilyCount = 0;
    private int metricCount = 0;

    private final Stopwatch stopwatch = Stopwatch.createUnstarted();


    public TextFormatChunkedInput(final Stream<MetricFamily> metricFamilies, final Instant timestamp, final Labels globalLabels, final boolean includeHelp) {
        this.metricFamilyIterator = metricFamilies.iterator();
        this.timestamp = " " + Long.toString(timestamp.toEpochMilli());
        this.globalLabels = globalLabels;
        this.includeHelp = includeHelp;
    }

    @Override
    public boolean isEndOfInput() throws Exception {
        return state == State.EOF;
    }

    @Override
    public void close() throws Exception {}


    public static ByteBuf formatLabels(final Map<String, String> labels) {
        final StringBuilder stringBuilder = new StringBuilder();

        if (labels.isEmpty())
            return Unpooled.EMPTY_BUFFER;

        final Iterator<Map.Entry<String, String>> labelsIterator = labels.entrySet().iterator();

        while (labelsIterator.hasNext()) {
            final Map.Entry<String, String> label = labelsIterator.next();

            stringBuilder.append(label.getKey())
                    .append("=\"")
                    .append(LABEL_VALUE_ESCAPER.escape(label.getValue()))
                    .append('"');

            if (labelsIterator.hasNext()) {
                stringBuilder.append(',');
            }
        }

        return ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, stringBuilder);
    }


    class MetricFamilyWriter {
        private final Consumer<ExpositionSink> headerWriter;
        private final Function<ExpositionSink, Boolean> metricWriter;

        class HeaderVisitor implements MetricFamilyVisitor<Consumer<ExpositionSink>> {
            private void writeFamilyHeader(final MetricFamily metricFamily, final ExpositionSink sink, final MetricFamilyType type) {
                sink.writeByte('\n');

                // # HELP <family name> <help>\n
                if (includeHelp && metricFamily.help != null) {
                    sink.writeAscii("# HELP ");
                    sink.writeAscii(metricFamily.name);
                    sink.writeByte(' ');
                    sink.writeUtf8(HELP_STRING_ESCAPER.escape(metricFamily.help));
                    sink.writeByte('\n');
                }

                // # TYPE <family name> <type> \n
                sink.writeAscii("# TYPE ");
                sink.writeAscii(metricFamily.name);
                sink.writeByte(' ');
                type.write(sink);
                sink.writeByte('\n');
            }

            private Consumer<ExpositionSink> forType(final MetricFamily metricFamily, final MetricFamilyType type) {
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
            private void writeLabels(final ExpositionSink sink, final Labels labels, final boolean commaPrefix) {
                if (commaPrefix) {
                    sink.writeByte(',');
                }

                sink.writeBytes(labels.asPlainTextFormatUTF8EncodedByteBuf().nioBuffer());
            }

            private void writeLabelSets(final ExpositionSink sink, final Labels... labelSets) {
                sink.writeByte('{');

                boolean needsComma = false;

                for (final Labels labels : labelSets) {
                    if (labels == null || labels.isEmpty())
                        continue;

                    writeLabels(sink, labels, needsComma);

                    needsComma = true;
                }

                if (!globalLabels.isEmpty()) {
                    writeLabels(sink, globalLabels, needsComma);
                }

                sink.writeByte('}');
            }

            private void writeMetric(final ExpositionSink sink, final MetricFamily metricFamily, final String suffix, final float value, final Labels... labelSets) {
                sink.writeAscii(metricFamily.name);
                if (suffix != null) {
                    sink.writeAscii(suffix);
                }

                writeLabelSets(sink, labelSets);

                sink.writeByte(' ');

                sink.writeFloat(value);
                sink.writeAscii(timestamp); // timestamp already has a leading space
                sink.writeByte('\n');
            }

            private <T extends Metric> Function<ExpositionSink, Boolean> metricWriter(final MetricFamily<T> metricFamily, final BiConsumer<T, ExpositionSink> writer) {
                try {
                    final Iterator<T> metricIterator = metricFamily.metrics().iterator();

                    return (buffer) -> {
                        if (metricIterator.hasNext()) {
                            writer.accept(metricIterator.next(), buffer);

                            return true;
                        }

                        return false;
                    };

                } catch (Exception e) {
                    throw e;
                }
            }

            @Override
            public Function<ExpositionSink, Boolean> visit(final CounterMetricFamily metricFamily) {
                return metricWriter(metricFamily, (counter, buffer) -> {
                    writeMetric(buffer, metricFamily, null, counter.value, counter.labels);
                });
            }

            @Override
            public Function<ExpositionSink, Boolean> visit(final GaugeMetricFamily metricFamily) {
                return metricWriter(metricFamily, (gauge, buffer) -> {
                    writeMetric(buffer, metricFamily, null, gauge.value, gauge.labels);
                });
            }

            @Override
            public Function<ExpositionSink, Boolean> visit(final SummaryMetricFamily metricFamily) {
                return metricWriter(metricFamily, (summary, buffer) -> {
                    writeMetric(buffer, metricFamily, "_sum", summary.sum, summary.labels);
                    writeMetric(buffer, metricFamily, "_count", summary.count, summary.labels);

                    summary.quantiles.forEach(interval -> {
                        writeMetric(buffer, metricFamily, null, interval.value, summary.labels, interval.quantile.asSummaryLabel());
                    });
                });
            }

            @Override
            public Function<ExpositionSink, Boolean> visit(final HistogramMetricFamily metricFamily) {
                return metricWriter(metricFamily, (histogram, buffer) -> {
                    writeMetric(buffer, metricFamily, "_sum", histogram.sum, histogram.labels);
                    writeMetric(buffer, metricFamily, "_count", histogram.count, histogram.labels);

                    histogram.buckets.forEach(interval -> {
                        writeMetric(buffer, metricFamily, "_bucket", interval.value, histogram.labels, interval.quantile.asHistogramLabel());
                    });

                    writeMetric(buffer, metricFamily, "_bucket", histogram.count, histogram.labels, Interval.Quantile.POSITIVE_INFINITY.asHistogramLabel());
                });
            }

            @Override
            public Function<ExpositionSink, Boolean> visit(final UntypedMetricFamily metricFamily) {
                return metricWriter(metricFamily, (untyped, buffer) -> {
                    writeMetric(buffer, metricFamily, null, untyped.value, untyped.labels);
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

        boolean writeMetric(final ExpositionSink sink) {
            return this.metricWriter.apply(sink);
        }
    }

    private void nextSlice(final ExpositionSink sink) throws Exception {
        switch (state) {
            case BANNER:
                stopwatch.start();

                sink.writeBytes(BANNER.nioBuffer());

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
                    state = State.METRIC_FAMILY;
                    return;
                }

                metricCount ++;

                return;

            case FOOTER:
                stopwatch.stop();
                sink.writeAscii("\n\n# Thanks and come again!\n\n");
                sink.writeAscii(String.format("# Wrote %s metrics for %s metric families in %s\n", metricCount, metricFamilyCount, stopwatch.toString()));

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
            try {
                nextSlice(new NettyExpositionSink(chunkBuffer));

            } catch (Exception e) {
                throw e;
            }
        }

        return chunkBuffer;
    }
}
