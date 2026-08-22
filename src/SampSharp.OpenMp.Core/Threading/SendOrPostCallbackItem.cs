using Microsoft.Extensions.ObjectPool;

namespace SampSharp.OpenMp.Core;

internal sealed class SendOrPostCallbackItem : IDisposable
{
    private readonly ManualResetEvent _asyncWaitHandle = new(false);
    private ExecutionType _executionType;
    private SendOrPostCallback? _method;
    private ObjectPool<SendOrPostCallbackItem>? _pool;
    private object? _state;

    public Exception? Exception { get; private set; }

    public WaitHandle ExecutionCompleteWaitHandle => _asyncWaitHandle;

    public void Dispose()
    {
        _asyncWaitHandle?.Dispose();
        GC.SuppressFinalize(this);
    }

    public void Set(ExecutionType executionType, SendOrPostCallback method, object? state, ObjectPool<SendOrPostCallbackItem>? returnToPool)
    {
        _executionType = executionType;
        _method = method;
        _state = state;
        _pool = returnToPool;
    }

    public void Reset()
    {
        _asyncWaitHandle.Reset();
        Exception = null;
        _method = null;
        _state = null;
        _pool = null;
    }

    public void Execute()
    {
        // Read the pool once, before the callback runs. On a Send item this field is
        // null and the waiting caller owns the return, but Set() below releases that
        // caller, which can return the item, let another thread take it out of the pool
        // and re-arm it with a non-null _pool, all while this frame is still on its way
        // to the finally. Returning to *that* pool would push a live, queued item back
        // into the pool and Reset() its callback out from under the pump, which then
        // dequeues it and throws a NullReferenceException whose only frame is this
        // method. Capturing first keeps this frame consistent with the item it ran.
        var pool = _pool;

        try
        {
            if (_executionType == ExecutionType.Send)
            {
                try
                {
                    _method!(_state);
                }
                catch (Exception e)
                {
                    Exception = e;
                }
                finally
                {
                    _asyncWaitHandle.Set();
                }
            }
            else
            {
                _method!(_state);
            }
        }
        finally
        {
            pool?.Return(this);
        }
    }
}