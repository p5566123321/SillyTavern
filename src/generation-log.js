import fs from 'node:fs';
import path from 'node:path';
import { Readable } from 'node:stream';
import { StringDecoder } from 'node:string_decoder';

const LOG_FILE_NAME = 'generations.jsonl';
const MAX_LOG_SIZE_BYTES = 5 * 1024 * 1024;
const TAIL_LENGTH = 80;

/**
 * Taps a streamed (SSE) Chat Completion response and appends a one-line JSON summary of the
 * generation to `<user root>/logs/generations.jsonl` when the stream ends, errors or is closed.
 * Records the provider-reported finish reason and token usage, so truncated replies can be
 * matched against the provider's own logs. Never alters the stream.
 * Must be called in the same tick as the stream is piped to the client, so no chunk is missed.
 * @param {import('node-fetch').Response} fetchResponse Upstream response
 * @param {object} meta Request metadata
 * @param {string} meta.directory User root directory
 * @param {string} meta.source Chat Completion source
 * @param {string} meta.model Requested model
 * @param {number} [meta.maxTokens] Requested max tokens
 * @param {string[]} [meta.providers] Requested provider order (OpenRouter)
 */
export function attachGenerationLogger(fetchResponse, { directory, source, model, maxTokens, providers }) {
    const body = fetchResponse?.body;
    if (!fetchResponse?.ok || !(body instanceof Readable) || !directory) {
        return;
    }

    const startedAt = Date.now();
    const decoder = new StringDecoder('utf8');
    const state = {
        id: null,
        provider: null,
        model: null,
        finishReason: null,
        nativeFinishReason: null,
        usage: null,
        error: null,
        contentChars: 0,
        reasoningChars: 0,
        tail: '',
    };
    let buffer = '';
    let finished = false;

    const handleLine = (/** @type {string} */ line) => {
        if (!line.startsWith('data:')) {
            return;
        }
        const data = line.slice(5).trim();
        if (!data || data === '[DONE]') {
            return;
        }
        let parsed;
        try {
            parsed = JSON.parse(data);
        } catch {
            return;
        }
        state.id ??= parsed.id ?? null;
        state.provider ??= parsed.provider ?? null;
        state.model ??= parsed.model ?? null;
        if (parsed.error) {
            state.error = parsed.error?.message ?? JSON.stringify(parsed.error);
        }
        if (parsed.usage) {
            state.usage = parsed.usage;
        }
        const choice = parsed.choices?.[0];
        if (!choice) {
            return;
        }
        const delta = choice.delta ?? {};
        if (typeof delta.content === 'string' && delta.content) {
            state.contentChars += delta.content.length;
            state.tail = (state.tail + delta.content).slice(-TAIL_LENGTH);
        }
        const reasoning = delta.reasoning ?? delta.reasoning_content;
        if (typeof reasoning === 'string') {
            state.reasoningChars += reasoning.length;
        }
        if (choice.finish_reason) {
            state.finishReason = choice.finish_reason;
        }
        if (choice.native_finish_reason) {
            state.nativeFinishReason = choice.native_finish_reason;
        }
        if (choice.error) {
            state.error = choice.error?.message ?? JSON.stringify(choice.error);
        }
    };

    const finish = async (/** @type {string} */ outcome) => {
        if (finished) {
            return;
        }
        finished = true;
        buffer += decoder.end();
        if (buffer) {
            handleLine(buffer);
        }

        const usage = state.usage ?? {};
        const entry = {
            time: new Date(startedAt).toISOString(),
            // complete: upstream ended the stream; closed: closed before the end (user stop, client gone, idle timeout); error: upstream stream error
            outcome,
            duration_ms: Date.now() - startedAt,
            source,
            requested_model: model,
            requested_providers: providers?.length ? providers : undefined,
            model: state.model,
            provider: state.provider,
            generation_id: state.id,
            finish_reason: state.finishReason,
            native_finish_reason: state.nativeFinishReason,
            max_tokens: maxTokens,
            prompt_tokens: usage.prompt_tokens,
            completion_tokens: usage.completion_tokens,
            reasoning_tokens: usage.completion_tokens_details?.reasoning_tokens,
            content_chars: state.contentChars,
            reasoning_chars: state.reasoningChars,
            tail: state.tail,
            error: state.error ?? undefined,
        };

        try {
            const logDir = path.join(directory, 'logs');
            const logFile = path.join(logDir, LOG_FILE_NAME);
            await fs.promises.mkdir(logDir, { recursive: true });
            const stat = await fs.promises.stat(logFile).catch(() => null);
            if (stat && stat.size > MAX_LOG_SIZE_BYTES) {
                await fs.promises.rename(logFile, `${logFile}.old`);
            }
            await fs.promises.appendFile(logFile, JSON.stringify(entry) + '\n', 'utf8');
        } catch (error) {
            console.warn('Failed to write generation log:', error);
        }
    };

    body.on('data', (/** @type {Buffer} */ chunk) => {
        buffer += decoder.write(chunk);
        const lines = buffer.split(/\r?\n/);
        buffer = lines.pop() ?? '';
        lines.forEach(handleLine);
    });
    body.on('end', () => finish('complete'));
    body.on('error', (error) => {
        state.error ??= error?.message ?? String(error);
        finish('error');
    });
    body.on('close', () => finish('closed'));
}
