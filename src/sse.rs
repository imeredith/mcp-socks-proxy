//! Incremental parser for `text/event-stream` bodies.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub event: String,
    pub data: String,
}

#[derive(Default)]
pub struct Parser {
    buf: Vec<u8>,
    event: String,
    data: String,
    has_data: bool,
    /// The last `id:` seen, kept across events as the spec requires.
    pub last_event_id: Option<String>,
}

impl Parser {
    /// Feeds a chunk of the body and returns the events it completes.
    pub fn push(&mut self, chunk: &[u8]) -> Vec<Event> {
        self.buf.extend_from_slice(chunk);
        let mut events = Vec::new();
        let mut start = 0;
        while let Some(offset) = self.buf[start..].iter().position(|&b| b == b'\n' || b == b'\r') {
            let end = start + offset;
            let next = match self.buf[end] {
                b'\r' if end + 1 == self.buf.len() => break, // might be half of a \r\n
                b'\r' if self.buf[end + 1] == b'\n' => end + 2,
                _ => end + 1,
            };
            let line = String::from_utf8_lossy(&self.buf[start..end]).into_owned();
            start = next;
            if let Some(event) = self.line(&line) {
                events.push(event);
            }
        }
        self.buf.drain(..start);
        events
    }

    fn line(&mut self, line: &str) -> Option<Event> {
        if line.is_empty() {
            let event = std::mem::take(&mut self.event);
            if !std::mem::take(&mut self.has_data) {
                return None;
            }
            let mut data = std::mem::take(&mut self.data);
            data.pop(); // trailing newline
            let event = if event.is_empty() { "message".to_owned() } else { event };
            return Some(Event { event, data });
        }
        if line.starts_with(':') {
            return None;
        }
        let (field, value) = match line.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (line, ""),
        };
        match field {
            "data" => {
                self.data.push_str(value);
                self.data.push('\n');
                self.has_data = true;
            }
            "event" => self.event = value.to_owned(),
            "id" if !value.contains('\0') => self.last_event_id = Some(value.to_owned()),
            _ => {}
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(data: &str) -> Event {
        Event { event: "message".into(), data: data.into() }
    }

    #[test]
    fn parses_events_split_across_chunks() {
        let mut p = Parser::default();
        assert_eq!(p.push(b"id: 7\nda"), vec![]);
        assert_eq!(p.push(b"ta: {\"a\":\r"), vec![]);
        assert_eq!(p.push(b"\ndata: 1}\r\n\r\n"), vec![message("{\"a\":\n1}")]);
        assert_eq!(p.last_event_id.as_deref(), Some("7"));
    }

    #[test]
    fn handles_comments_event_types_and_empty_events() {
        let mut p = Parser::default();
        let events = p.push(b": keepalive\n\nevent: endpoint\ndata:/messages\n\nid: 1\n\ndata\n\n");
        assert_eq!(events, vec![Event { event: "endpoint".into(), data: "/messages".into() }, message(""),]);
    }
}
