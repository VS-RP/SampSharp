using System.Collections.Concurrent;

namespace SampSharp.OpenMp.Core;

internal sealed class SampSharpSynchronizationContext : SynchronizationContext
{
    // Items are NOT pooled. Pooling recycled an item back into circulation before
    // the pump thread had finished with it, so it could reach Execute() after
    // Reset() (callback == null) — a race that stays rare under light load but
    // STORMS under heavy main-thread marshalling. A fresh single-use item per
    // call is inherently race-free; the allocation is negligible next to the
    // cross-thread hop it represents.
    private readonly ConcurrentQueue<SendOrPostCallbackItem> _queue = new();

    public override void Send(SendOrPostCallback d, object? state)
    {
        using var item = new SendOrPostCallbackItem();
        item.Set(ExecutionType.Send, d, state, null);

        _queue.Enqueue(item);

        item.ExecutionCompleteWaitHandle.WaitOne();

        var ex = item.Exception;

        if (ex != null)
        {
            throw ex;
        }
    }

    public override void Post(SendOrPostCallback d, object? state)
    {
        // Fire-and-forget: the item disposes itself in Execute() (it carries a null
        // pool, so Execute() frees rather than returns it).
        var item = new SendOrPostCallbackItem();
        item.Set(ExecutionType.Post, d, state, null);
        _queue.Enqueue(item);
    }

    public override SynchronizationContext CreateCopy()
    {
        // Do not copy
        return this;
    }

    public SendOrPostCallbackItem? GetMessage()
    {
        _queue.TryDequeue(out var result);
        return result;
    }
}